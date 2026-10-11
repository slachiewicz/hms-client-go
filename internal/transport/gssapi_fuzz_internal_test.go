package transport

import (
	"crypto/rand"
	"testing"

	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/stretchr/testify/require"
)

// The fuzz targets below feed bytes an untrusted metastore or KDC controls
// to every parser they reach before authentication completes. Each must
// return an error for garbage, never panic. `go test` runs only the seeds;
// run one with `go test -fuzz=FuzzVerifyAPRep ./internal/transport/`.

// fuzzMech returns a mechanism holding the fixture's session key and an
// authenticator, the state verifyAPRep and unwrap check a reply against,
// along with a valid AP_REP and wrap token to seed the corpus.
func fuzzMech(t testing.TB) (*gssapiMech, []byte, []byte) {
	t.Helper()
	tt, ok := t.(*testing.T)
	if !ok {
		t.Fatal("fuzzMech needs a *testing.T")
	}
	fixture := newKrbFixture(tt, "metastore.example.com")
	auth, err := types.NewAuthenticator(testRealm, fixture.creds.CName())
	require.NoError(t, err)
	a := &gssAcceptor{fixture: fixture}
	apRep, _, err := a.apRepToken(auth, fixture.sessionKey)
	require.NoError(t, err)
	wrapTok, err := a.wrap(fixture.sessionKey, []byte{qopAuth, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
	m := &gssapiMech{auth: auth, sessionKey: fixture.sessionKey, ctxKey: fixture.sessionKey}
	return m, apRep, wrapTok
}

func FuzzVerifyAPRep(f *testing.F) {
	m, apRep, _ := fuzzMech(&testing.T{})
	f.Add(apRep)
	f.Add(apRep[:len(apRep)/2])
	f.Add([]byte{asn1AppTag0, 0x84, 0x7f, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		mech := *m
		_ = mech.verifyAPRep(b)
	})
}

func FuzzUnwrap(f *testing.F) {
	m, _, wrapTok := fuzzMech(&testing.T{})
	f.Add(wrapTok)
	f.Add(wrapTok[:16])
	f.Fuzz(func(t *testing.T, b []byte) {
		mech := *m
		_, _ = mech.unwrap(b)
	})
}

// FuzzKDCReply covers what gokrb5 parses from a KDC during the AS and TGS
// exchanges, and the decryption every encrypted part goes through, for
// each encryption type it supports.
func FuzzKDCReply(f *testing.F) {
	f.Add([]byte{0x6b, 0x03, 0x30, 0x01, 0x00})
	f.Add([]byte{0x6d, 0x00})
	f.Add([]byte{0x7e, 0x82, 0xff, 0xff})
	etypes := []int32{
		etypeID.AES128_CTS_HMAC_SHA1_96, etypeID.AES256_CTS_HMAC_SHA1_96,
		etypeID.AES128_CTS_HMAC_SHA256_128, etypeID.AES256_CTS_HMAC_SHA384_192,
		etypeID.RC4_HMAC, etypeID.DES3_CBC_SHA1_KD,
	}
	keys := make([]types.EncryptionKey, 0, len(etypes))
	for _, id := range etypes {
		et, err := crypto.GetEtype(id)
		if err != nil {
			f.Fatal(err)
		}
		v := make([]byte, et.GetKeyByteSize())
		_, _ = rand.Read(v)
		keys = append(keys, types.EncryptionKey{KeyType: id, KeyValue: v})
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var as messages.ASRep
		_ = as.Unmarshal(b)
		var tgs messages.TGSRep
		_ = tgs.Unmarshal(b)
		var kerr messages.KRBError
		_ = kerr.Unmarshal(b)
		for _, k := range keys {
			_, _ = decryptEncPart(types.EncryptedData{EType: k.KeyType, Cipher: b}, k, keyusage.AS_REP_ENCPART)
		}
	})
}
