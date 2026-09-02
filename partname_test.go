package hms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
)

// TestPartitionName covers PartitionName's escaping against known Hive
// FileUtils.escapePathName/Warehouse.makePartName outputs (SPEC §5.5, G9):
// a plain value round-trips unchanged, a space is left alone (only
// Shell.WINDOWS escapes it, which this package's Unix-only rule set does
// not model), a character in Hive's clash set is percent-encoded in
// uppercase hex, an empty value becomes the default partition name, and a
// key is lowercased before it is escaped, matching
// Warehouse.makePartName's own lowercasing.
func TestPartitionName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		keys   []string
		values []string
		want   string
	}{
		{"plain value is unchanged", []string{"dt"}, []string{"2024-01-01"}, "dt=2024-01-01"},
		{"space is not escaped", []string{"region"}, []string{"a b"}, "region=a b"},
		{"slash is escaped", []string{"dt"}, []string{"a/b"}, "dt=a%2Fb"},
		{"equals is escaped", []string{"dt"}, []string{"x=y"}, "dt=x%3Dy"},
		{"empty value becomes the default partition name", []string{"dt"}, []string{""}, "dt=__HIVE_DEFAULT_PARTITION__"},
		{"multiple keys are joined with /", []string{"dt", "region"}, []string{"2024-01-01", "eu"}, "dt=2024-01-01/region=eu"},
		{"key is lowercased", []string{"DT"}, []string{"2024-01-01"}, "dt=2024-01-01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := hms.PartitionName(tt.keys, tt.values)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPartitionName_LengthMismatch covers PartitionName's error return: a
// caller passing mismatched keys/values slices is a programming error, not
// a partial result.
func TestPartitionName_LengthMismatch(t *testing.T) {
	t.Parallel()
	_, err := hms.PartitionName([]string{"dt", "region"}, []string{"2024-01-01"})
	require.Error(t, err)
}
