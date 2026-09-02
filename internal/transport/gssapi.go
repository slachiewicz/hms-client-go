package transport

import (
	"bytes"
	"context"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/flags"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
)

// defaultServiceClass is the service class of the metastore's Kerberos
// principal, as in "hive/<host>@<REALM>"; it is what Hive's
// hive.metastore.kerberos.principal defaults to.
const defaultServiceClass = "hive"

// asn1AppTag0 is the DER identifier octet for [APPLICATION 0] constructed,
// the wrapper a GSS-API initial context token carries (RFC 2743 §3.1).
const asn1AppTag0 byte = 0x60

// qopAuth is the SASL GSSAPI security layer bit for auth, meaning no
// security layer (RFC 4752 §3.3; the other bits are 2 for integrity and 4
// for confidentiality). Only auth is supported: under it the data frames
// that follow the handshake are plain length-prefixed frames, exactly as
// under SASL PLAIN, so no per-frame wrapping is needed.
const qopAuth byte = 1

// GSS-API wrap token flags (RFC 4121 §4.2.2).
const (
	wrapFlagSentByAcceptor byte = 1
	wrapFlagSealed         byte = 2
	wrapFlagAcceptorSubkey byte = 4
)

// GSS-API KRB5 mech token identifiers (RFC 4121 §4.1).
var (
	tokIDAPReq    = [2]byte{0x01, 0x00}
	tokIDAPRep    = [2]byte{0x02, 0x00}
	tokIDKRBError = [2]byte{0x03, 0x00}
)

// gssContextFlags are the GSS-API context flags advertised in the
// authenticator's checksum: mutual authentication (so the server proves it
// holds the service key, which the AP_REP verification below relies on)
// and integrity, matching what Hive's Java client requests.
var gssContextFlags = []uint32{gssapi.ContextFlagMutual, gssapi.ContextFlagInteg}

// KerberosConfig configures SASL GSSAPI authentication over the binary TCP
// transport (SPEC §3.1). It selects the client's Kerberos identity and how
// its credentials are obtained; the metastore's own principal is derived
// from the endpoint host unless ServicePrincipal overrides it.
type KerberosConfig struct {
	// Principal is the client principal, either "user" or
	// "user@REALM". When it carries no realm, the realm defaults to
	// krb5.conf's default_realm. It is required with Keytab and ignored
	// with CCache, whose client principal comes from the cache itself.
	Principal string
	// Keytab is the path to a keytab holding Principal's key. When set,
	// the client authenticates to the KDC with it and CCache is ignored.
	Keytab string
	// CCache is the path to a credential cache to read the client's TGT
	// from. When both Keytab and CCache are empty, the cache named by
	// KRB5CCNAME is used, falling back to /tmp/krb5cc_<uid>.
	CCache string
	// Krb5Conf is the path to the Kerberos configuration file. When
	// empty, KRB5_CONFIG is used, falling back to /etc/krb5.conf.
	Krb5Conf string
	// ServicePrincipal overrides the metastore's service principal name.
	// When empty it is "hive/<host>", with <host> the endpoint host
	// DialBinary was given; the realm is resolved from krb5.conf's
	// domain_realm mapping, as for any SPN without one.
	ServicePrincipal string

	// initiator, when non-nil, supplies the client's Kerberos identity and
	// service tickets instead of loading them from Keytab or CCache. It is
	// a test hook (see gssapi_internal_test.go), which is why it is
	// unexported: it lets the handshake be exercised end to end against a
	// ticket minted in-process, with no KDC. Production dials always leave
	// it nil.
	initiator initiator
}

// initiator supplies the Kerberos identity and service tickets that a
// GSSAPI handshake needs, so the handshake itself never depends on how the
// credentials were obtained (keytab, credential cache, or a test fixture).
type initiator interface {
	// Credentials returns the client's Kerberos credentials, whose
	// principal name and realm go into the AP_REQ's authenticator.
	Credentials() *credentials.Credentials
	// ServiceTicket returns a service ticket for spn and the session key
	// shared with the service, acquiring it from the KDC if it is not
	// already cached.
	ServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error)
}

// krbInitiator adapts a gokrb5 client to initiator.
type krbInitiator struct{ cl *client.Client }

// Credentials returns the gokrb5 client's credentials.
func (k krbInitiator) Credentials() *credentials.Credentials { return k.cl.Credentials }

// ServiceTicket returns a service ticket for spn, performing the AS and
// TGS exchanges with the KDC when the client has no cached ticket.
func (k krbInitiator) ServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error) {
	return k.cl.GetServiceTicket(spn)
}

// NewSaslGSSAPI wraps inner in a SaslTransport that authenticates with SASL
// GSSAPI (RFC 4752) on Open, in the same Java TSaslTransport framing
// NewSaslPlain uses. hostPort is the metastore endpoint, whose host half
// supplies the default service principal.
//
// Only QOP auth is negotiated (SPEC §3.1): the client tells the server it
// wants no security layer, so the data frames that follow the handshake are
// unwrapped, and a server that offers no such layer fails the handshake
// rather than silently downgrading.
//
// Loading the caller's credentials (keytab or credential cache) happens
// here, so a misconfigured WithKerberos fails before any handshake I/O.
// The KDC exchanges that acquire the service ticket happen in the
// handshake itself, on Open.
func NewSaslGSSAPI(inner thrift.TTransport, hostPort string, cfg KerberosConfig) (SaslTransport, error) {
	spn, err := servicePrincipal(hostPort, cfg.ServicePrincipal)
	if err != nil {
		return nil, err
	}
	init := cfg.initiator
	if init == nil {
		cl, err := newKrbClient(cfg)
		if err != nil {
			return nil, err
		}
		init = krbInitiator{cl: cl}
	}
	return &saslTransport{inner: inner, mech: &gssapiMech{init: init, spn: spn}}, nil
}

// servicePrincipal returns the metastore's service principal name:
// override when the caller supplied one, otherwise "hive/<host>" built
// from the endpoint.
func servicePrincipal(hostPort, override string) (string, error) {
	if override != "" {
		return override, nil
	}
	host := hostPort
	if h, _, err := net.SplitHostPort(hostPort); err == nil {
		host = h
	}
	if host == "" {
		return "", errors.New("hms: kerberos: no endpoint host to build the service principal from")
	}
	return defaultServiceClass + "/" + host, nil
}

// gssStage is the round the GSSAPI handshake is on.
type gssStage int

// The rounds of the SASL GSSAPI handshake, in order.
const (
	// gssStageAPReq sends the AP_REQ initial context token.
	gssStageAPReq gssStage = iota
	// gssStageAPRep verifies the server's AP_REP (mutual authentication).
	gssStageAPRep
	// gssStageSecLayer answers the server's security layer offer.
	gssStageSecLayer
	// gssStageDone means the mechanism has sent its last token.
	gssStageDone
)

// gssapiMech is the SASL GSSAPI client mechanism (RFC 4752) for Hive's
// TSaslTransport framing. It runs three rounds: an AP_REQ initial context
// token, the server's AP_REP (verified for mutual authentication), and the
// security layer negotiation, whose reply is the mechanism's last token.
type gssapiMech struct {
	init initiator
	spn  string

	stage gssStage
	// auth is the authenticator sent inside the AP_REQ; the AP_REP must
	// echo its timestamp for mutual authentication to hold.
	auth types.Authenticator
	// sessionKey is the service ticket's session key, which decrypts the
	// AP_REP.
	sessionKey types.EncryptionKey
	// ctxKey is the key the security layer's wrap tokens are signed and
	// verified with: the acceptor's subkey when the AP_REP carries one,
	// otherwise sessionKey.
	ctxKey types.EncryptionKey
	// acceptorSubkey records that ctxKey came from the AP_REP, which the
	// wrap tokens must flag (RFC 4121 §4.2.2).
	acceptorSubkey bool
	// sendSeq is the sequence number of the next wrap token this client
	// sends; it starts at the authenticator's sequence number.
	sendSeq uint64
}

// Name returns the SASL mechanism name sent in the START frame.
func (*gssapiMech) Name() string { return "GSSAPI" }

// Complete reports whether every round has run. Until it does, a COMPLETE
// from the server is a protocol violation the driver rejects: a peer that
// answered the AP_REQ with COMPLETE would otherwise skip the AP_REP, and
// with it the proof that the peer holds the service key.
func (m *gssapiMech) Complete() bool { return m.stage == gssStageDone }

// Step advances the GSSAPI handshake by one round. See gssStage for the
// rounds; only the last one is reported done, since the server answers it
// with COMPLETE.
func (m *gssapiMech) Step(ctx context.Context, challenge []byte) ([]byte, bool, error) {
	switch m.stage {
	case gssStageAPReq:
		token, err := m.initialToken(ctx)
		if err != nil {
			return nil, false, err
		}
		m.stage = gssStageAPRep
		return token, false, nil
	case gssStageAPRep:
		if err := m.verifyAPRep(challenge); err != nil {
			return nil, false, err
		}
		m.stage = gssStageSecLayer
		// The GSS context is established; RFC 4752 leaves the client
		// nothing to send until the server offers its security layers,
		// so this round's response is empty, as in Java's GssKrb5Client.
		return nil, false, nil
	case gssStageSecLayer:
		token, err := m.selectSecurityLayer(challenge)
		if err != nil {
			return nil, false, err
		}
		m.stage = gssStageDone
		return token, true, nil
	default:
		return nil, true, errors.New("hms: sasl gssapi: unexpected challenge after the handshake completed")
	}
}

// initialToken acquires the service ticket and returns the GSS-API initial
// context token holding an AP_REQ with the mutual-required option set.
func (m *gssapiMech) initialToken(ctx context.Context) ([]byte, error) {
	tkt, key, err := m.serviceTicket(ctx)
	if err != nil {
		return nil, err
	}
	m.sessionKey = key

	creds := m.init.Credentials()
	auth, err := types.NewAuthenticator(creds.Domain(), creds.CName())
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: building the authenticator: %w", err)
	}
	auth.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  gssChecksum(gssContextFlags),
	}
	// A KerberosTime carries whole seconds only (the microseconds travel
	// separately, in Cusec), so the timestamp the AP_REP echoes back can
	// never be finer than that. Truncating here keeps the value compared in
	// verifyAPRep identical to the one that goes on the wire.
	auth.CTime = auth.CTime.Truncate(time.Second)
	apReq, err := messages.NewAPReq(tkt, key, auth)
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: building the AP_REQ: %w", err)
	}
	types.SetFlag(&apReq.APOptions, flags.APOptionMutualRequired)

	if auth.SeqNumber < 0 {
		return nil, errors.New("hms: kerberos: negative authenticator sequence number")
	}
	m.auth = auth
	m.sendSeq = uint64(auth.SeqNumber)

	b, err := apReq.Marshal()
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: marshalling the AP_REQ: %w", err)
	}
	return krb5Token(tokIDAPReq, b)
}

// serviceTicket acquires the service ticket under ctx. The AS and TGS
// exchanges it may run reach the KDC over gokrb5's own sockets, which
// gokrb5 gives no way to bind a context to, so the exchange runs on its own
// goroutine and ctx cancellation abandons it rather than waiting out
// krb5.conf's kdc_timeout. The result channel is buffered so the abandoned
// goroutine still finishes and is collected.
func (m *gssapiMech) serviceTicket(ctx context.Context) (messages.Ticket, types.EncryptionKey, error) {
	type acquired struct {
		ticket messages.Ticket
		key    types.EncryptionKey
		err    error
	}
	done := make(chan acquired, 1)
	go func() {
		ticket, key, err := m.init.ServiceTicket(m.spn)
		done <- acquired{ticket: ticket, key: key, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			return messages.Ticket{}, types.EncryptionKey{}, fmt.Errorf("hms: kerberos: no service ticket for %s: %w", m.spn, got.err)
		}
		return got.ticket, got.key, nil
	case <-ctx.Done():
		return messages.Ticket{}, types.EncryptionKey{}, ctx.Err()
	}
}

// verifyAPRep checks the server's reply to the AP_REQ. The reply proves
// the server holds the service key: only that key decrypts the ticket, and
// only the session key inside it encrypts an AP_REP that echoes the
// authenticator's timestamp. An AP_REP subkey, when present, becomes the
// key the security layer's wrap tokens use.
func (m *gssapiMech) verifyAPRep(token []byte) error {
	tokID, msg, err := parseKrb5Token(token)
	if err != nil {
		return err
	}
	switch tokID {
	case tokIDKRBError:
		var kerr messages.KRBError
		if err := kerr.Unmarshal(msg); err != nil {
			return fmt.Errorf("hms: kerberos: the server rejected the AP_REQ with an unreadable error: %w", err)
		}
		return fmt.Errorf("hms: kerberos: the server rejected the AP_REQ: %s", kerr.Error())
	case tokIDAPRep:
		// The expected reply; parsed below.
	default:
		return fmt.Errorf("hms: kerberos: expected an AP_REP from the server, got token 0x%02x%02x", tokID[0], tokID[1])
	}
	var apRep messages.APRep
	if err := apRep.Unmarshal(msg); err != nil {
		return fmt.Errorf("hms: kerberos: could not parse the AP_REP: %w", err)
	}
	plain, err := crypto.DecryptEncPart(apRep.EncPart, m.sessionKey, keyusage.AP_REP_ENCPART)
	if err != nil {
		return fmt.Errorf("hms: kerberos: could not decrypt the AP_REP: %w", err)
	}
	var enc messages.EncAPRepPart
	if err := enc.Unmarshal(plain); err != nil {
		return fmt.Errorf("hms: kerberos: could not parse the AP_REP: %w", err)
	}
	if !enc.CTime.Equal(m.auth.CTime) || enc.Cusec != m.auth.Cusec {
		return errors.New("hms: kerberos: mutual authentication failed: the AP_REP does not echo the authenticator's timestamp")
	}
	m.ctxKey = m.sessionKey
	if enc.Subkey.KeyType != 0 && len(enc.Subkey.KeyValue) > 0 {
		m.ctxKey = enc.Subkey
		m.acceptorSubkey = true
	}
	return nil
}

// selectSecurityLayer answers the server's wrapped security layer offer
// (RFC 4752 §3.1): a bit mask of the layers it supports and, in the
// remaining three octets, the largest message it can receive. The client
// selects auth -- no security layer -- and fails when the server does not
// offer it, rather than negotiating a layer whose per-frame wrapping this
// transport does not implement.
func (m *gssapiMech) selectSecurityLayer(challenge []byte) ([]byte, error) {
	offer, err := m.unwrap(challenge)
	if err != nil {
		return nil, err
	}
	if len(offer) != 4 {
		return nil, fmt.Errorf("hms: kerberos: security layer offer is %d bytes, want 4", len(offer))
	}
	if offer[0]&qopAuth == 0 {
		return nil, fmt.Errorf("hms: kerberos: the server offers security layers 0x%02x but not auth; "+
			"set the metastore's hadoop.rpc.protection to authentication", offer[0])
	}
	// The three size octets are the largest message this client can
	// receive. Selecting no security layer means nothing is ever wrapped,
	// so there is no such message and the field is zero, as Java's
	// GssKrb5Client sends it.
	return m.wrap([]byte{qopAuth, 0, 0, 0})
}

// wrap returns payload in a GSS-API wrap token signed with the context key
// (RFC 4121 §4.2.6.2). The token is integrity-protected only: this client
// never asks for confidentiality, so the payload is not encrypted.
func (m *gssapiMech) wrap(payload []byte) ([]byte, error) {
	sumLen, err := checksumLen(m.ctxKey)
	if err != nil {
		return nil, err
	}
	var tokenFlags byte
	if m.acceptorSubkey {
		tokenFlags |= wrapFlagAcceptorSubkey
	}
	token := gssapi.WrapToken{
		Flags:     tokenFlags,
		EC:        sumLen,
		RRC:       0,
		SndSeqNum: m.sendSeq,
		Payload:   payload,
	}
	if err := token.SetCheckSum(m.ctxKey, keyusage.GSSAPI_INITIATOR_SEAL); err != nil {
		return nil, fmt.Errorf("hms: kerberos: signing a wrap token: %w", err)
	}
	m.sendSeq++
	b, err := token.Marshal()
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: marshalling a wrap token: %w", err)
	}
	return b, nil
}

// checksumLen returns the length in octets of the checksum a wrap token
// signed with key carries, which is the token's EC field.
func checksumLen(key types.EncryptionKey) (uint16, error) {
	et, err := crypto.GetEtype(key.KeyType)
	if err != nil {
		return 0, fmt.Errorf("hms: kerberos: unsupported encryption type %d: %w", key.KeyType, err)
	}
	bits := et.GetHMACBitLength()
	if bits < 0 || bits/8 > 0xFFFF {
		return 0, fmt.Errorf("hms: kerberos: implausible checksum length %d bits", bits)
	}
	return uint16(bits / 8), nil
}

// unwrap verifies a wrap token from the server and returns its payload.
func (m *gssapiMech) unwrap(b []byte) ([]byte, error) {
	normalized, err := unrotateWrapToken(b)
	if err != nil {
		return nil, err
	}
	var token gssapi.WrapToken
	if err := token.Unmarshal(normalized, true); err != nil {
		return nil, fmt.Errorf("hms: kerberos: the server sent a malformed wrap token: %w", err)
	}
	if token.Flags&wrapFlagSentByAcceptor == 0 {
		return nil, errors.New("hms: kerberos: the server sent a wrap token marked as coming from the initiator")
	}
	if token.Flags&wrapFlagSealed != 0 {
		return nil, errors.New("hms: kerberos: the server sent an encrypted wrap token; only integrity protection is supported")
	}
	ok, err := token.Verify(m.ctxKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: the server's wrap token failed verification: %w", err)
	}
	if !ok {
		return nil, errors.New("hms: kerberos: the server's wrap token failed verification")
	}
	return token.Payload, nil
}

// unrotateWrapToken undoes the right rotation a sender may apply to a wrap
// token's data (RFC 4121 §4.2.5): the RRC header field counts the octets
// the payload and checksum were rotated by. It returns a copy of b with
// the rotation reversed and RRC cleared, since gssapi.WrapToken.Unmarshal
// splits payload from checksum by length alone and would otherwise mis-cut
// a rotated token.
func unrotateWrapToken(b []byte) ([]byte, error) {
	if len(b) < gssapi.HdrLen {
		return nil, errors.New("hms: kerberos: wrap token shorter than its header")
	}
	data := b[gssapi.HdrLen:]
	out := make([]byte, len(b))
	copy(out, b)
	binary.BigEndian.PutUint16(out[6:8], 0)
	if len(data) == 0 {
		return out, nil
	}
	rrc := int(binary.BigEndian.Uint16(b[6:8])) % len(data)
	copy(out[gssapi.HdrLen:], data[rrc:])
	copy(out[gssapi.HdrLen+len(data)-rrc:], data[:rrc])
	return out, nil
}

// krb5Token wraps a Kerberos message in a GSS-API initial context token
// (RFC 2743 §3.1, RFC 4121 §4.1): an [APPLICATION 0] wrapper around the
// KRB5 mechanism OID, a two byte token identifier, and the message. This
// is the raw Kerberos 5 mechanism token, not a SPNEGO negotiation token:
// SASL names the mechanism itself, so there is nothing to negotiate.
func krb5Token(tokID [2]byte, msg []byte) ([]byte, error) {
	oid, err := asn1.Marshal(asn1.ObjectIdentifier(gssapi.OIDKRB5.OID()))
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: marshalling the KRB5 mechanism OID: %w", err)
	}
	b := make([]byte, 0, len(oid)+len(tokID)+len(msg))
	b = append(b, oid...)
	b = append(b, tokID[:]...)
	b = append(b, msg...)
	return asn1tools.AddASNAppTag(b, 0), nil
}

// parseKrb5Token is krb5Token's inverse: it unwraps a GSS-API initial
// context token and returns its token identifier and the Kerberos message
// inside. The wrapper is decoded by hand rather than through an ASN.1
// unmarshaller because only its two headers are ASN.1: what follows the
// mechanism OID is raw token bytes, not a DER element the decoder could
// describe.
func parseKrb5Token(b []byte) (tokID [2]byte, msg []byte, err error) {
	if len(b) < 2 || b[0] != asn1AppTag0 {
		return tokID, nil, errors.New("hms: kerberos: the server did not return a GSS-API context token")
	}
	body, err := derContent(b)
	if err != nil {
		return tokID, nil, err
	}
	oid, err := asn1.Marshal(asn1.ObjectIdentifier(gssapi.OIDKRB5.OID()))
	if err != nil {
		return tokID, nil, fmt.Errorf("hms: kerberos: marshalling the KRB5 mechanism OID: %w", err)
	}
	if len(body) < len(oid)+2 || !bytes.Equal(body[:len(oid)], oid) {
		return tokID, nil, errors.New("hms: kerberos: the server's token is not a Kerberos 5 mechanism token")
	}
	copy(tokID[:], body[len(oid):len(oid)+2])
	return tokID, body[len(oid)+2:], nil
}

// derContent returns the content octets of the single DER element b starts
// with, skipping its identifier and length octets. Only the single-byte
// identifiers this package emits and receives are accepted.
func derContent(b []byte) ([]byte, error) {
	const malformed = "hms: kerberos: malformed GSS-API token"
	if len(b) < 2 {
		return nil, errors.New(malformed)
	}
	length := int(b[1])
	start := 2
	if length > 0x7F {
		// Long form: the low seven bits count the length octets that follow.
		n := length & 0x7F
		if n == 0 || n > 4 || len(b) < 2+n {
			return nil, errors.New(malformed)
		}
		length = 0
		for _, c := range b[2 : 2+n] {
			length = length<<8 | int(c)
		}
		start = 2 + n
	}
	if length < 0 || start+length > len(b) {
		return nil, errors.New(malformed)
	}
	return b[start : start+length], nil
}

// gssChecksum builds the authenticator checksum RFC 4121 §4.1.1 defines
// for the Kerberos 5 GSS-API mechanism: a four octet length of 16, sixteen
// octets of channel bindings (unused, hence zero), and the context flags.
func gssChecksum(contextFlags []uint32) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint32(b[:4], 16)
	var f uint32
	for _, flag := range contextFlags {
		f |= flag
	}
	binary.LittleEndian.PutUint32(b[20:24], f)
	return b
}

// newKrbClient builds a gokrb5 client from cfg: from a keytab when one is
// configured, otherwise from a credential cache (SPEC §5.1, WithKerberos).
func newKrbClient(cfg KerberosConfig) (*client.Client, error) {
	confPath := cfg.Krb5Conf
	if confPath == "" {
		confPath = os.Getenv("KRB5_CONFIG")
	}
	if confPath == "" {
		confPath = "/etc/krb5.conf"
	}
	krbConf, err := config.Load(confPath)
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: loading %s: %w", confPath, err)
	}

	if cfg.Keytab != "" {
		kt, err := keytab.Load(cfg.Keytab)
		if err != nil {
			return nil, fmt.Errorf("hms: kerberos: loading keytab %s: %w", cfg.Keytab, err)
		}
		user, realm := splitPrincipal(cfg.Principal, krbConf.LibDefaults.DefaultRealm)
		if user == "" {
			return nil, errors.New("hms: kerberos: a client principal is required when a keytab is used")
		}
		return client.NewWithKeytab(user, realm, kt, krbConf), nil
	}

	path := cfg.CCache
	if path == "" {
		if path, err = defaultCCachePath(); err != nil {
			return nil, err
		}
	}
	cc, err := credentials.LoadCCache(path)
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: loading credential cache %s: %w", path, err)
	}
	cl, err := client.NewFromCCache(cc, krbConf)
	if err != nil {
		return nil, fmt.Errorf("hms: kerberos: reading credential cache %s: %w", path, err)
	}
	return cl, nil
}

// splitPrincipal splits "user@REALM" into its halves, defaulting the realm
// to krb5.conf's default_realm when the principal carries none.
func splitPrincipal(principal, defaultRealm string) (user, realm string) {
	user, realm, found := strings.Cut(principal, "@")
	if !found || realm == "" {
		return user, defaultRealm
	}
	return user, realm
}

// defaultCCachePath returns the credential cache to use when none was
// configured: KRB5CCNAME when set, otherwise /tmp/krb5cc_<uid>.
//
// KRB5CCNAME may name a cache type as well as a location ("FILE:/path",
// "DIR:/path", "KEYRING:persistent:0", "KCM:"). Only FILE is readable
// here, since gokrb5 parses the on-disk FILE format and nothing else, so
// any other type is reported rather than silently misread as a path.
func defaultCCachePath() (string, error) {
	v := os.Getenv("KRB5CCNAME")
	if v == "" {
		return "/tmp/krb5cc_" + strconv.Itoa(os.Getuid()), nil
	}
	kind, rest, found := strings.Cut(v, ":")
	if !found || !isCCacheType(kind) {
		// No type prefix: the whole value is a path.
		return v, nil
	}
	if !strings.EqualFold(kind, "FILE") {
		return "", fmt.Errorf("hms: kerberos: KRB5CCNAME names credential cache type %q, "+
			"which this client cannot read; use a FILE: cache, or name one with WithKerberos", kind)
	}
	return rest, nil
}

// isCCacheType reports whether s looks like a KRB5CCNAME cache type rather
// than the first segment of a path: a run of letters, as every type MIT
// defines is.
func isCCacheType(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

// Validate reports whether cfg can be used to authenticate: that its
// Kerberos configuration and credentials are readable and that a principal
// is present when one is required. It is what lets a caller mistake -- a
// keytab path with a typo in it, say -- surface from hms.New as an invalid
// operation, before any endpoint is dialed, instead of as a dial failure.
func (cfg KerberosConfig) Validate() error {
	if cfg.initiator != nil {
		return nil
	}
	_, err := newKrbClient(cfg)
	return err
}
