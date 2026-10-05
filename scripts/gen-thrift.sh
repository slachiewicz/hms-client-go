#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
IDL_DIR="${ROOT_DIR}/idl"
GEN_DIR="${ROOT_DIR}/gen"

HIVE_VERSION="rel/release-4.2.1"
HIVE_RAW_BASE="https://raw.githubusercontent.com/apache/hive/${HIVE_VERSION}/standalone-metastore/metastore-common/src/main/thrift"

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

echo "==> Preparing directories..."
# hive_metastore.thrift does `include "share/fb303/if/fb303.thrift"`, so fb303
# must live at that relative path next to it.
mkdir -p "${IDL_DIR}/share/fb303/if" "${GEN_DIR}"

echo "==> Fetching Hive ${HIVE_VERSION} and fb303 (thrift v${THRIFT_VERSION}) IDL..."
curl -sSfL "${FB303_RAW}" -o "${IDL_DIR}/share/fb303/if/fb303.thrift"
curl -sSfL "${HIVE_RAW_BASE}/hive_metastore.thrift" -o "${IDL_DIR}/hive_metastore.thrift"

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
