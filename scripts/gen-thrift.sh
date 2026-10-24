#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
IDL_DIR="${ROOT_DIR}/idl"
GEN_DIR="${ROOT_DIR}/gen"

HIVE_VERSION="rel/release-4.2.1"
HIVE_RAW_BASE="https://raw.githubusercontent.com/apache/hive/${HIVE_VERSION}/standalone-metastore/metastore-common/src/main/thrift"

# SHA-256 of each IDL file at the versions above. A changed file fails the
# run rather than silently changing gen/. Bumping HIVE_VERSION or the thrift
# version in go.mod means updating the matching hash here.
HIVE_IDL_SHA256="a389ab04d7b85b5647f63942105cecacde1c43660b24c63fd070aa8fe2f639ca"
FB303_IDL_SHA256="6c4ee5aa9a0b01980e5ce4c00073da8b93dc50fed5f7f2b5743ff0b9468d176e"

# Must match github.com/apache/thrift in go.mod: generated code and runtime
# library have to be the same minor version.
THRIFT_VERSION="$(cd "${ROOT_DIR}" && go list -m -f '{{.Version}}' github.com/apache/thrift 2>/dev/null | sed -E 's/^v//')"
if [[ -z "${THRIFT_VERSION}" ]]; then
  echo "error: could not read github.com/apache/thrift version from go.mod" >&2
  exit 1
fi
FB303_RAW="https://raw.githubusercontent.com/apache/thrift/v${THRIFT_VERSION}/contrib/fb303/if/fb303.thrift"

INSTALLED="$(thrift -version 2>/dev/null | sed -nE 's/^Thrift version ([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
if [[ "${INSTALLED}" != "${THRIFT_VERSION}" ]]; then
  echo "error: thrift compiler ${INSTALLED:-not found} does not match go.mod library ${THRIFT_VERSION}" >&2
  exit 1
fi

# fetch downloads $1 to $2 and fails unless its SHA-256 is $3.
fetch() {
  local tmp got
  tmp="$(mktemp)"
  curl -sSfL "$1" -o "${tmp}"
  if command -v sha256sum >/dev/null; then
    got="$(sha256sum "${tmp}" | cut -d' ' -f1)"
  else
    got="$(shasum -a 256 "${tmp}" | cut -d' ' -f1)"
  fi
  if [[ "${got}" != "$3" ]]; then
    rm -f "${tmp}"
    echo "error: $1 has SHA-256 ${got}, expected $3" >&2
    exit 1
  fi
  mv "${tmp}" "$2"
}

echo "==> Preparing directories..."
# hive_metastore.thrift does `include "share/fb303/if/fb303.thrift"`, so fb303
# must live at that relative path next to it.
mkdir -p "${IDL_DIR}/share/fb303/if" "${GEN_DIR}"

echo "==> Fetching Hive ${HIVE_VERSION} and fb303 (thrift v${THRIFT_VERSION}) IDL..."
fetch "${FB303_RAW}" "${IDL_DIR}/share/fb303/if/fb303.thrift" "${FB303_IDL_SHA256}"
fetch "${HIVE_RAW_BASE}/hive_metastore.thrift" "${IDL_DIR}/hive_metastore.thrift" "${HIVE_IDL_SHA256}"

echo "==> Compiling Thrift bindings..."
rm -rf "${GEN_DIR:?}"/*
thrift -r --gen "go:package_prefix=github.com/slachiewicz/hms-client-go/gen/" \
  -out "${GEN_DIR}" "${IDL_DIR}/hive_metastore.thrift"

# The generator also emits a `-remote` CLI main package per service; they are
# not part of the library and would put package main inside the module.
find "${GEN_DIR}" -type d -name '*-remote' -prune -exec rm -rf {} +

echo "==> Formatting generated Go code..."
gofmt -w "${GEN_DIR}"

echo "✓ Thrift generation complete."
