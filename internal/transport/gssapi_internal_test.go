package transport

// This file is package transport (white-box), not transport_test, because
// the handshake it drives runs without a KDC: the service ticket and the
// client credentials are minted in-process and reach the mechanism through
// KerberosConfig's unexported initiator hook. Reaching that hook, and the
// negotiation-frame helpers the fake acceptor shares with the client, is
// the only reason these tests live inside the package.

import (
	"context"
	"crypto/rand"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/iana/flags"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/service"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/gen/fb303"
)

// The fake Kerberos realm these tests run in.
const (
	testRealm           = "TEST.HMSCLIENT"
	testClientPrincipal = "hms-client"
	testServiceKey      = "the-service-key"
	// testEtype is the encryption type the service key, the ticket, and
	// every wrap token below use.
	testEtype = etypeID.AES256_CTS_HMAC_SHA1_96
	// testServerTimeout bounds the fake acceptor's socket I/O so a broken
	// handshake fails the test instead of hanging it.
	testServerTimeout = 30 * time.Second
)

// krbFixture is a Kerberos world with no KDC in it: a service keytab, a
// client identity, and a service ticket minted straight from the keytab.
// That is everything the SASL GSSAPI handshake needs on either side, since
// the KDC's only role is issuing the ticket the fixture already holds.
type krbFixture struct {
	keytab     *keytab.Keytab
	creds      *credentials.Credentials
	ticket     messages.Ticket
	sessionKey types.EncryptionKey
	spn        string
}

// newKrbFixture builds a fixture whose service principal is "hive/<host>",
// the principal the client derives from an endpoint on that host.
func newKrbFixture(t *testing.T, host string) *krbFixture {
	t.Helper()
	spn := "hive/" + host
	kt := keytab.New()
	require.NoError(t, kt.AddEntry(spn, testRealm, testServiceKey, time.Now(), 1, testEtype))

	creds := credentials.New(testClientPrincipal, testRealm)
	now := time.Now().UTC()
	tkt, key, err := messages.NewTicket(
		creds.CName(), creds.Domain(),
		types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, spn), testRealm,
		types.NewKrbFlags(), kt, testEtype, 1,
		now, now, now.Add(time.Hour), now.Add(2*time.Hour),
	)
	require.NoError(t, err)
	return &krbFixture{keytab: kt, creds: creds, ticket: tkt, sessionKey: key, spn: spn}
}

// Credentials satisfies initiator.
func (f *krbFixture) Credentials() *credentials.Credentials { return f.creds }

// ServiceTicket satisfies initiator, standing in for the TGS exchange.
func (f *krbFixture) ServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error) {
	if spn != f.spn {
		return messages.Ticket{}, types.EncryptionKey{}, fmt.Errorf("no ticket for %q, only %q", spn, f.spn)
	}
	return f.ticket, f.sessionKey, nil
}

// testEncAPRepPart and testAPRep mirror the AP_REP structures RFC 4120
// §5.5.2 defines. They are declared here, rather than reused from gokrb5,
// so the acceptor encodes its reply with the standard library's ASN.1
// marshaller: the client's parser is then exercised against an encoding it
// did not itself produce, and the optional fields can be omitted or
// included per test.
type testEncAPRepPart struct {
	CTime          time.Time           `asn1:"generalized,explicit,tag:0"`
	Cusec          int                 `asn1:"explicit,tag:1"`
	Subkey         types.EncryptionKey `asn1:"optional,explicit,tag:2"`
	SequenceNumber int64               `asn1:"optional,explicit,tag:3"`
}

type testAPRep struct {
	PVNO    int                 `asn1:"explicit,tag:0"`
	MsgType int                 `asn1:"explicit,tag:1"`
	EncPart types.EncryptedData `asn1:"explicit,tag:2"`
}

// gssAcceptor is a fake Hive-side SASL GSSAPI acceptor: it verifies the
// client's AP_REQ against the fixture's keytab with gokrb5's own service
// package, answers with an AP_REP, and runs the RFC 4752 security layer
// negotiation. Its fields bend one step of that exchange at a time.
type gssAcceptor struct {
	fixture *krbFixture

	// rejectWith, when non-empty, answers the AP_REQ with a BAD frame
	// carrying it, as a server whose own checks failed would.
	rejectWith string
	// apRepKey overrides the key the AP_REP is encrypted with, standing in
	// for a server that does not hold the service key.
	apRepKey *types.EncryptionKey
	// apRepSkew shifts the timestamp the AP_REP echoes back, breaking
	// mutual authentication.
	apRepSkew time.Duration
	// subkey has the AP_REP carry an acceptor subkey, which then keys the
	// security layer's wrap tokens instead of the ticket's session key.
	subkey bool
	// layers is the security layer bit mask offered to the client.
	layers byte
	// rrc rotates the offer's wrap token right by that many octets, as RFC
	// 4121 §4.2.5 permits.
	rrc uint16
	// corruptOffer flips a byte of the offer's payload after it is signed.
	corruptOffer bool

	// selected records the security layer the client chose. It is read
	// only after the acceptor's goroutine has finished.
	selected byte
}

// run performs the acceptor's half of the handshake over s, using the same
// negotiation framing the client does.
func (a *gssAcceptor) run(ctx context.Context, s *saslTransport) error {
	status, payload, err := s.recvNegotiate()
	if err != nil {
		return err
	}
	if status != saslStart {
		return fmt.Errorf("expected START, got status %d", status)
	}
	if string(payload) != "GSSAPI" {
		return fmt.Errorf("expected mechanism GSSAPI, got %q", payload)
	}

	status, payload, err = s.recvNegotiate()
	if err != nil {
		return err
	}
	if status != saslOK {
		return fmt.Errorf("expected OK with the initial response, got status %d", status)
	}
	apReq, err := a.verifyAPReq(payload)
	if err != nil {
		return err
	}
	if a.rejectWith != "" {
		return s.sendNegotiate(ctx, saslBad, []byte(a.rejectWith))
	}

	sessionKey := apReq.Ticket.DecryptedEncPart.Key
	token, ctxKey, err := a.apRepToken(apReq.Authenticator, sessionKey)
	if err != nil {
		return err
	}
	if err := s.sendNegotiate(ctx, saslOK, token); err != nil {
		return err
	}

	status, payload, err = s.recvNegotiate()
	if err != nil {
		return err
	}
	if status != saslOK || len(payload) != 0 {
		return fmt.Errorf("expected an empty OK after the AP_REP, got status %d with %d bytes", status, len(payload))
	}

	offer, err := a.wrap(ctxKey, []byte{a.layers, 0xFF, 0xFF, 0xFF})
	if err != nil {
		return err
	}
	if err := s.sendNegotiate(ctx, saslOK, offer); err != nil {
		return err
	}

	status, payload, err = s.recvNegotiate()
	if err != nil {
		return err
	}
	if status != saslComplete {
		return fmt.Errorf("expected COMPLETE with the layer selection, got status %d", status)
	}
	selection, err := a.unwrap(ctxKey, payload)
	if err != nil {
		return err
	}
	if len(selection) != 4 {
		return fmt.Errorf("layer selection is %d bytes, want 4", len(selection))
	}
	a.selected = selection[0]
	return s.sendNegotiate(ctx, saslComplete, nil)
}

// verifyAPReq unwraps the client's initial context token and validates the
// AP_REQ inside it against the fixture's keytab.
func (a *gssAcceptor) verifyAPReq(token []byte) (*messages.APReq, error) {
	tokID, msg, err := parseKrb5Token(token)
	if err != nil {
		return nil, err
	}
	if tokID != tokIDAPReq {
		return nil, fmt.Errorf("expected an AP_REQ, got token 0x%02x%02x", tokID[0], tokID[1])
	}
	var apReq messages.APReq
	if err := apReq.Unmarshal(msg); err != nil {
		return nil, err
	}
	if !types.IsFlagSet(&apReq.APOptions, flags.APOptionMutualRequired) {
		return nil, errors.New("the client did not request mutual authentication")
	}
	settings := service.NewSettings(a.fixture.keytab, service.DecodePAC(false))
	ok, _, err := service.VerifyAPREQ(&apReq, settings)
	if err != nil {
		return nil, fmt.Errorf("AP_REQ verification failed: %w", err)
	}
	if !ok {
		return nil, errors.New("AP_REQ verification failed")
	}
	return &apReq, nil
}

// apRepToken builds the AP_REP answering auth and returns it alongside the
// key the security layer's wrap tokens are to use.
func (a *gssAcceptor) apRepToken(auth types.Authenticator, sessionKey types.EncryptionKey) ([]byte, types.EncryptionKey, error) {
	enc := testEncAPRepPart{CTime: auth.CTime.Add(a.apRepSkew), Cusec: auth.Cusec}
	ctxKey := sessionKey
	if a.subkey {
		value := make([]byte, len(sessionKey.KeyValue))
		if _, err := rand.Read(value); err != nil {
			return nil, ctxKey, err
		}
		ctxKey = types.EncryptionKey{KeyType: sessionKey.KeyType, KeyValue: value}
		enc.Subkey = ctxKey
	}
	plain, err := asn1.MarshalWithParams(enc, "application,explicit,tag:27")
	if err != nil {
		return nil, ctxKey, err
	}
	key := sessionKey
	if a.apRepKey != nil {
		key = *a.apRepKey
	}
	ed, err := crypto.GetEncryptedData(plain, key, keyusage.AP_REP_ENCPART, 0)
	if err != nil {
		return nil, ctxKey, err
	}
	b, err := asn1.MarshalWithParams(testAPRep{PVNO: 5, MsgType: 15, EncPart: ed}, "application,explicit,tag:15")
	if err != nil {
		return nil, ctxKey, err
	}
	token, err := krb5Token(tokIDAPRep, b)
	return token, ctxKey, err
}

// wrap signs payload as a wrap token from the acceptor, optionally rotated
// and optionally corrupted after signing.
func (a *gssAcceptor) wrap(key types.EncryptionKey, payload []byte) ([]byte, error) {
	sumLen, err := checksumLen(key)
	if err != nil {
		return nil, err
	}
	tokenFlags := wrapFlagSentByAcceptor
	if a.subkey {
		tokenFlags |= wrapFlagAcceptorSubkey
	}
	token := gssapi.WrapToken{
		Flags:     tokenFlags,
		EC:        sumLen,
		SndSeqNum: 0,
		Payload:   payload,
	}
	if err := token.SetCheckSum(key, keyusage.GSSAPI_ACCEPTOR_SEAL); err != nil {
		return nil, err
	}
	b, err := token.Marshal()
	if err != nil {
		return nil, err
	}
	if a.corruptOffer {
		b[gssapi.HdrLen] ^= 0xFF
	}
	return rotateWrapToken(b, a.rrc), nil
}

// unwrap verifies a wrap token from the client and returns its payload.
func (a *gssAcceptor) unwrap(key types.EncryptionKey, b []byte) ([]byte, error) {
	var token gssapi.WrapToken
	if err := token.Unmarshal(b, false); err != nil {
		return nil, err
	}
	ok, err := token.Verify(key, keyusage.GSSAPI_INITIATOR_SEAL)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("the client's wrap token failed verification")
	}
	return token.Payload, nil
}

// rotateWrapToken rotates a wrap token's data right by rrc octets and
// records the count in the header, the transformation unrotateWrapToken
// reverses.
func rotateWrapToken(b []byte, rrc uint16) []byte {
	data := b[gssapi.HdrLen:]
	if len(data) == 0 {
		return b
	}
	n := int(rrc) % len(data)
	out := make([]byte, len(b))
	copy(out, b[:gssapi.HdrLen])
	binary.BigEndian.PutUint16(out[6:8], rrc)
	copy(out[gssapi.HdrLen:], data[len(data)-n:])
	copy(out[gssapi.HdrLen+n:], data[:len(data)-n])
	return out
}

// serveGSSAPI listens on a fresh loopback socket, runs a's half of the
// handshake on the first connection, and then, when call is set, answers
// one fb303 getStatus call over the resulting transport. The returned
// channel carries the acceptor's error, if any, and is closed when it is
// done.
func serveGSSAPI(t *testing.T, ln net.Listener, a *gssAcceptor, call bool) <-chan error {
	t.Helper()
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		conn, err := ln.Accept()
		if err != nil {
			errs <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(testServerTimeout))

		ctx := context.Background()
		s := &saslTransport{inner: thrift.NewTSocketFromConnConf(conn, &thrift.TConfiguration{})}
		if err := a.run(ctx, s); err != nil {
			errs <- err
			return
		}
		if !call {
			return
		}
		// The handshake is complete, so the transport now carries plain
		// length-prefixed data frames in both directions.
		s.open = true
		if err := serveGetStatus(ctx, s); err != nil {
			errs <- err
		}
	}()
	return errs
}

// serveGetStatus answers one fb303 getStatus call with ALIVE, hand-rolled
// rather than routed through the generated processor so the test needs no
// implementation of the whole FacebookService interface.
func serveGetStatus(ctx context.Context, s *saslTransport) error {
	proto := thrift.NewTBinaryProtocolConf(s, &thrift.TConfiguration{})
	name, _, seqID, err := proto.ReadMessageBegin(ctx)
	if err != nil {
		return err
	}
	if name != "getStatus" {
		return fmt.Errorf("expected a getStatus call, got %q", name)
	}
	if err := proto.Skip(ctx, thrift.STRUCT); err != nil {
		return err
	}
	if err := proto.ReadMessageEnd(ctx); err != nil {
		return err
	}
	if err := proto.WriteMessageBegin(ctx, name, thrift.REPLY, seqID); err != nil {
		return err
	}
	if err := proto.WriteStructBegin(ctx, "getStatus_result"); err != nil {
		return err
	}
	if err := proto.WriteFieldBegin(ctx, "success", thrift.I32, 0); err != nil {
		return err
	}
	if err := proto.WriteI32(ctx, int32(fb303.FbStatus_ALIVE)); err != nil {
		return err
	}
	if err := proto.WriteFieldEnd(ctx); err != nil {
		return err
	}
	if err := proto.WriteFieldStop(ctx); err != nil {
		return err
	}
	if err := proto.WriteStructEnd(ctx); err != nil {
		return err
	}
	if err := proto.WriteMessageEnd(ctx); err != nil {
		return err
	}
	return proto.Flush(ctx)
}

// newTestListener returns a listener on loopback and the host half of its
// address, which is also the host the client builds its service principal
// from.
func newTestListener(t *testing.T) (net.Listener, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	host, _, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	return ln, host
}

// drain waits for the acceptor's goroutine to finish, discarding whatever
// it reported: once the client has failed the handshake and hung up, the
// acceptor's own next read fails too, and that failure is not the subject
// of the test.
func drain(t *testing.T, errs <-chan error) {
	t.Helper()
	for range errs { //nolint:revive // the values are deliberately discarded
	}
}

func TestGSSAPIHandshakeAndCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bend func(*gssAcceptor)
	}{
		{
			name: "ticket session key",
			bend: func(*gssAcceptor) {},
		},
		{
			name: "acceptor subkey keys the security layer",
			bend: func(a *gssAcceptor) { a.subkey = true },
		},
		{
			name: "rotated wrap token",
			bend: func(a *gssAcceptor) { a.rrc = 13 },
		},
		{
			name: "every security layer offered",
			bend: func(a *gssAcceptor) { a.layers = qopAuth | 2 | 4 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ln, host := newTestListener(t)
			fixture := newKrbFixture(t, host)
			acceptor := &gssAcceptor{fixture: fixture, layers: qopAuth}
			tc.bend(acceptor)
			errs := serveGSSAPI(t, ln, acceptor, true)

			ctx, cancel := context.WithTimeout(t.Context(), testServerTimeout)
			defer cancel()
			conn, err := DialBinary(ctx, ln.Addr().String(), BinaryConfig{
				Kerberos: &KerberosConfig{initiator: fixture},
			})
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()

			status, err := fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)
			require.NoError(t, err)
			assert.Equal(t, fb303.FbStatus_ALIVE, status)

			require.NoError(t, <-errs)
			assert.Equal(t, qopAuth, acceptor.selected, "the client must select auth, the only layer it implements")
		})
	}
}

func TestGSSAPIHandshakeFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bend    func(*gssAcceptor, *krbFixture)
		wantErr string
	}{
		{
			name:    "server rejects the AP_REQ",
			bend:    func(a *gssAcceptor, _ *krbFixture) { a.rejectWith = "GSS initiate failed" },
			wantErr: "GSS initiate failed",
		},
		{
			name: "AP_REP encrypted with the wrong key",
			bend: func(a *gssAcceptor, f *krbFixture) {
				wrong := make([]byte, len(f.sessionKey.KeyValue))
				wrong[0] = 0xA5
				a.apRepKey = &types.EncryptionKey{KeyType: f.sessionKey.KeyType, KeyValue: wrong}
			},
			wantErr: "could not decrypt the AP_REP",
		},
		{
			name:    "AP_REP echoes the wrong timestamp",
			bend:    func(a *gssAcceptor, _ *krbFixture) { a.apRepSkew = time.Second },
			wantErr: "mutual authentication failed",
		},
		{
			name:    "server offers no auth layer",
			bend:    func(a *gssAcceptor, _ *krbFixture) { a.layers = 2 | 4 },
			wantErr: "offers security layers 0x06 but not auth",
		},
		{
			name:    "security layer offer fails verification",
			bend:    func(a *gssAcceptor, _ *krbFixture) { a.corruptOffer = true },
			wantErr: "failed verification",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ln, host := newTestListener(t)
			fixture := newKrbFixture(t, host)
			acceptor := &gssAcceptor{fixture: fixture, layers: qopAuth}
			tc.bend(acceptor, fixture)
			errs := serveGSSAPI(t, ln, acceptor, false)

			ctx, cancel := context.WithTimeout(t.Context(), testServerTimeout)
			defer cancel()
			_, err := DialBinary(ctx, ln.Addr().String(), BinaryConfig{
				Kerberos: &KerberosConfig{initiator: fixture},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			drain(t, errs)
		})
	}
}

func TestGSSAPIRejectsUnknownServicePrincipal(t *testing.T) {
	t.Parallel()
	ln, host := newTestListener(t)
	fixture := newKrbFixture(t, host)
	acceptor := &gssAcceptor{fixture: fixture, layers: qopAuth}
	errs := serveGSSAPI(t, ln, acceptor, false)

	ctx, cancel := context.WithTimeout(t.Context(), testServerTimeout)
	defer cancel()
	_, err := DialBinary(ctx, ln.Addr().String(), BinaryConfig{
		Kerberos: &KerberosConfig{initiator: fixture, ServicePrincipal: "hive/elsewhere.invalid"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no service ticket for hive/elsewhere.invalid")
	drain(t, errs)
}

func TestDialBinaryRejectsBothSaslMechanisms(t *testing.T) {
	t.Parallel()
	_, err := DialBinary(t.Context(), "127.0.0.1:1", BinaryConfig{
		PlainUser: "alice",
		Kerberos:  &KerberosConfig{Principal: "alice@" + testRealm},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestServicePrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostPort string
		override string
		want     string
		wantErr  string
	}{
		{name: "host and port", hostPort: "hms.example.com:9083", want: "hive/hms.example.com"},
		{name: "bare host", hostPort: "hms.example.com", want: "hive/hms.example.com"},
		{name: "ipv6", hostPort: "[::1]:9083", want: "hive/::1"},
		{name: "override wins", hostPort: "hms.example.com:9083", override: "hive/other@REALM", want: "hive/other@REALM"},
		{name: "no host", hostPort: ":9083", wantErr: "no endpoint host"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := servicePrincipal(tc.hostPort, tc.override)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSplitPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		want      string
		wantRealm string
	}{
		{name: "bare name takes the default realm", principal: "alice", want: "alice", wantRealm: "EXAMPLE.COM"},
		{name: "qualified name keeps its realm", principal: "alice@OTHER.COM", want: "alice", wantRealm: "OTHER.COM"},
		{name: "empty realm falls back", principal: "alice@", want: "alice", wantRealm: "EXAMPLE.COM"},
		{name: "empty principal", principal: "", want: "", wantRealm: "EXAMPLE.COM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			user, realm := splitPrincipal(tc.principal, "EXAMPLE.COM")
			assert.Equal(t, tc.want, user)
			assert.Equal(t, tc.wantRealm, realm)
		})
	}
}

// TestDefaultCCachePath does not call t.Parallel: t.Setenv forbids it.
func TestDefaultCCachePath(t *testing.T) {
	t.Setenv("KRB5CCNAME", "FILE:/var/run/krb5cc_test")
	assert.Equal(t, "/var/run/krb5cc_test", defaultCCachePath())

	t.Setenv("KRB5CCNAME", "/var/run/plain")
	assert.Equal(t, "/var/run/plain", defaultCCachePath())

	t.Setenv("KRB5CCNAME", "")
	assert.Equal(t, fmt.Sprintf("/tmp/krb5cc_%d", os.Getuid()), defaultCCachePath())
}

func TestNewSaslGSSAPICredentialErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	krb5Conf := filepath.Join(dir, "krb5.conf")
	require.NoError(t, os.WriteFile(krb5Conf, []byte(
		"[libdefaults]\n default_realm = "+testRealm+"\n\n[realms]\n "+testRealm+" = {\n  kdc = 127.0.0.1:88\n }\n"), 0o600))

	tests := []struct {
		name    string
		cfg     KerberosConfig
		wantErr string
	}{
		{
			name:    "missing krb5.conf",
			cfg:     KerberosConfig{Krb5Conf: filepath.Join(dir, "absent.conf"), Keytab: filepath.Join(dir, "absent.keytab")},
			wantErr: "loading " + filepath.Join(dir, "absent.conf"),
		},
		{
			name:    "missing keytab",
			cfg:     KerberosConfig{Krb5Conf: krb5Conf, Principal: "alice", Keytab: filepath.Join(dir, "absent.keytab")},
			wantErr: "loading keytab",
		},
		{
			name:    "keytab without a principal",
			cfg:     KerberosConfig{Krb5Conf: krb5Conf, Keytab: writeTestKeytab(t, dir)},
			wantErr: "a client principal is required",
		},
		{
			name:    "missing credential cache",
			cfg:     KerberosConfig{Krb5Conf: krb5Conf, CCache: filepath.Join(dir, "absent.ccache")},
			wantErr: "loading credential cache",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSaslGSSAPI(nil, "hms.example.com:9083", tc.cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// writeTestKeytab writes a one-entry keytab and returns its path.
func writeTestKeytab(t *testing.T, dir string) string {
	t.Helper()
	kt := keytab.New()
	require.NoError(t, kt.AddEntry("alice", testRealm, "secret", time.Now(), 1, testEtype))
	b, err := kt.Marshal()
	require.NoError(t, err)
	path := filepath.Join(dir, "client.keytab")
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return path
}

func TestUnrotateWrapToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rrc     uint16
		token   []byte
		wantErr string
	}{
		{name: "no rotation", rrc: 0},
		{name: "rotated by one", rrc: 1},
		{name: "rotated by the payload length", rrc: 8},
		{name: "rotation wraps around", rrc: 21},
		{name: "short token", token: make([]byte, gssapi.HdrLen-1), wantErr: "shorter than its header"},
		{name: "header only", token: make([]byte, gssapi.HdrLen)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.token != nil {
				got, err := unrotateWrapToken(tc.token)
				if tc.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tc.wantErr)
					return
				}
				require.NoError(t, err)
				assert.Len(t, got, len(tc.token))
				return
			}
			data := make([]byte, 16)
			for i := range data {
				data[i] = byte(i + 1)
			}
			original := append(make([]byte, gssapi.HdrLen), data...)
			got, err := unrotateWrapToken(rotateWrapToken(original, tc.rrc))
			require.NoError(t, err)
			assert.Equal(t, original, got)
		})
	}
}

func TestKrb5TokenRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  []byte
	}{
		{name: "short message, short form length", msg: []byte("kerberos")},
		{name: "long message, long form length", msg: make([]byte, 500)},
		{name: "empty message", msg: []byte{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			token, err := krb5Token(tokIDAPReq, tc.msg)
			require.NoError(t, err)
			tokID, msg, err := parseKrb5Token(token)
			require.NoError(t, err)
			assert.Equal(t, tokIDAPReq, tokID)
			assert.Equal(t, tc.msg, msg)
		})
	}
}

func TestParseKrb5TokenRejectsGarbage(t *testing.T) {
	t.Parallel()

	valid, err := krb5Token(tokIDAPRep, []byte("payload"))
	require.NoError(t, err)

	tests := []struct {
		name    string
		token   []byte
		wantErr string
	}{
		{name: "empty", token: nil, wantErr: "did not return a GSS-API context token"},
		{name: "wrong application tag", token: []byte{0x30, 0x02, 0x00, 0x00}, wantErr: "did not return a GSS-API context token"},
		{name: "truncated body", token: valid[:len(valid)-4], wantErr: "malformed GSS-API token"},
		{name: "not a kerberos mechanism", token: []byte{0x60, 0x03, 0x06, 0x01, 0x2a}, wantErr: "not a Kerberos 5 mechanism token"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := parseKrb5Token(tc.token)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
