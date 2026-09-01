package hms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	hms "github.com/slachiewicz/hms-client-go"
)

// TestConn_FallbackCache exercises the per-method legacy-fallback cache
// (SPEC §2.3 Rules 3 and 4) that Task 11's retry/failover loop reads and
// writes through useLegacy / markLegacy: a method starts out not legacy,
// markLegacy flips it for the rest of the conn's lifetime, and other
// methods are unaffected.
func TestConn_FallbackCache(t *testing.T) {
	t.Parallel()
	cn := hms.NewTestConn()

	assert.False(t, hms.ConnUseLegacy(cn, "get_partitions_req"))

	hms.ConnMarkLegacy(cn, "get_partitions_req")
	assert.True(t, hms.ConnUseLegacy(cn, "get_partitions_req"))

	// A different method's cache entry is unaffected.
	assert.False(t, hms.ConnUseLegacy(cn, "alter_partitions_req"))
}
