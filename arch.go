package hms

import "strconv"

// hms-client-go supports 64-bit platforms only (SPEC §1.1). On a platform
// where int is narrower than 64 bits this constant is negative and does not
// fit a uint, so the package fails to compile there instead of running with
// length arithmetic that was never written or tested for a 32-bit int.
const _ uint = strconv.IntSize - 64
