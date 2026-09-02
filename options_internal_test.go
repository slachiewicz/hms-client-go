package hms

// This file is package hms (white-box), not hms_test, because what
// WithKerberos promises in SPEC §5.1 -- how its optional second argument is
// classified, and that it suppresses set_ugi -- is visible only on the
// unexported config the Option writes to.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithKerberos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principal     string
		credentials   []string
		wantKeytab    string
		wantCCache    string
		wantPrincipal string
	}{
		{
			name:          "no credentials uses the ambient cache",
			principal:     "alice@EXAMPLE.COM",
			wantPrincipal: "alice@EXAMPLE.COM",
		},
		{
			name:          "a .keytab path is a keytab",
			principal:     "alice",
			credentials:   []string{"/etc/security/alice.keytab"},
			wantKeytab:    "/etc/security/alice.keytab",
			wantPrincipal: "alice",
		},
		{
			name:          "any other path is a credential cache",
			principal:     "alice",
			credentials:   []string{"/tmp/krb5cc_1000"},
			wantCCache:    "/tmp/krb5cc_1000",
			wantPrincipal: "alice",
		},
		{
			name:          "an empty path is no path at all",
			principal:     "alice",
			credentials:   []string{""},
			wantPrincipal: "alice",
		},
		{
			name:          "extra arguments are ignored",
			principal:     "alice",
			credentials:   []string{"/tmp/cc", "/etc/other.keytab"},
			wantCCache:    "/tmp/cc",
			wantPrincipal: "alice",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newConfig()
			WithKerberos(tc.principal, tc.credentials...)(cfg)

			assert.True(t, cfg.kerberos)
			assert.Equal(t, tc.wantPrincipal, cfg.krbPrincipal)
			assert.Equal(t, tc.wantKeytab, cfg.krbKeytab)
			assert.Equal(t, tc.wantCCache, cfg.krbCCache)

			krb := krbConfig(cfg)
			if assert.NotNil(t, krb) {
				assert.Equal(t, tc.wantPrincipal, krb.Principal)
				assert.Equal(t, tc.wantKeytab, krb.Keytab)
				assert.Equal(t, tc.wantCCache, krb.CCache)
			}
		})
	}
}

func TestWantsSetUgi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
		want bool
	}{
		{name: "no user at all", want: false},
		{name: "user alone", opts: []Option{WithUser("alice")}, want: true},
		{
			name: "SASL PLAIN establishes the identity instead",
			opts: []Option{WithUser("alice"), WithPlainAuth("alice", "s3cret")},
			want: false,
		},
		{
			name: "Kerberos establishes the identity instead",
			opts: []Option{WithUser("alice"), WithKerberos("alice@EXAMPLE.COM")},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newConfig()
			for _, opt := range tc.opts {
				opt(cfg)
			}
			assert.Equal(t, tc.want, cfg.wantsSetUgi())
		})
	}
}

func TestKrbConfigNilWithoutWithKerberos(t *testing.T) {
	t.Parallel()
	cfg := newConfig()
	WithPlainAuth("alice", "s3cret")(cfg)
	assert.Nil(t, krbConfig(cfg))
}
