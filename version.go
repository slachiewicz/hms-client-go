package hms

import "runtime/debug"

// modulePath is this module's import path, used to find its own version in
// debug.BuildInfo.
const modulePath = "github.com/slachiewicz/hms-client-go"

// moduleVersion returns this module's version as reported by
// debug.ReadBuildInfo: the version recorded for this module's own path among
// its dependents' dependencies, falling back to the main module's version
// when this module is the main module, and finally to "devel" when neither
// is available (e.g. when built without module information).
func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			return dep.Version
		}
	}
	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}

// userAgent returns the "User-Agent" header value sent on every HTTP
// request, per SPEC §3.2.
func userAgent() string {
	return "hms-client-go/" + moduleVersion()
}
