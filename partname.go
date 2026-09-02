package hms

import (
	"fmt"
	"strings"
)

// defaultPartitionName is Hive's built-in placeholder for a null or empty
// partition key or value ("hive.exec.default.partition.name"'s default),
// substituted by PartitionName the same way Hive's own
// FileUtils.escapePathName does.
const defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"

// PartitionName builds a Hive-style partition name
// ("key1=value1/key2=value2/...") from a table's partition keys and one
// partition's values, in the order both are given, for use with
// DropPartitions and anywhere else this package's opaque partition-name
// currency (as returned by GetPartitionNames/GetPartitionsByNames) is
// built rather than read off the wire. It returns an error if len(keys) !=
// len(values).
//
// This follows Hive's own Warehouse.makePartName / FileUtils.makePartName
// rules exactly, so the result matches what a real metastore computes for
// the same values: each key is lowercased (Warehouse.makePartName does
// this before ever reaching FileUtils), each key and value is escaped
// individually (FileUtils.escapePathName), and an empty key or value
// becomes "__HIVE_DEFAULT_PARTITION__" rather than an empty string on
// either side of "=".
func PartitionName(keys []string, values []string) (string, error) {
	if len(keys) != len(values) {
		return "", fmt.Errorf("hms: PartitionName: %d keys but %d values", len(keys), len(values))
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(escapePartitionPathName(strings.ToLower(k)))
		b.WriteByte('=')
		b.WriteString(escapePartitionPathName(values[i]))
	}
	return b.String(), nil
}

// escapePartitionPathName is Hive's FileUtils.escapePathName, Unix branch
// (Shell.WINDOWS's extra escapes for space, '<', '>', '|' do not apply: HMS
// paths are HDFS/object-store paths, not local Windows filesystem paths).
// An empty s becomes defaultPartitionName; otherwise every byte
// needsPartitionPathEscaping reports true for is replaced with "%XX"
// (uppercase hex), matching escapePathName's own char-at-a-time (not
// UTF-8-rune-at-a-time) encoding -- the same one Hive's server-side
// partition directory names, and so its own get_partition_names* output,
// already assume.
func escapePartitionPathName(s string) string {
	if s == "" {
		return defaultPartitionName
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if needsPartitionPathEscaping(c) {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// needsPartitionPathEscaping reports whether c is in Hive's
// FileUtils.charToEscape set (Unix branch): every ASCII control character
// (0x00-0x1F), DEL (0x7F), and the literal clash set double-quote, hash,
// percent, single-quote, asterisk, slash, colon, equals, question mark,
// backslash, and the brace/bracket/caret characters -- see the switch
// below for the exact set. Notably, space (0x20) is NOT in this set --
// only Shell.WINDOWS adds it, along with '<', '>', and '|', neither of
// which this package needs either.
func needsPartitionPathEscaping(c byte) bool {
	if c < ' ' || c == 0x7F {
		return true
	}
	switch c {
	case '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '{', '[', ']', '^':
		return true
	}
	return false
}
