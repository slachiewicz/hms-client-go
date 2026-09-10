#!/bin/bash
# Initialise the realm once, export the keytabs the other containers and
# the test runner need, then run the KDC in the foreground.
set -euo pipefail

REALM="${REALM:-EXAMPLE.COM}"
HMS_HOST="${HMS_HOST:-hms.example.com}"
KEYTAB_DIR="${KEYTAB_DIR:-/keytabs}"

if [ ! -f /var/lib/krb5kdc/principal ]; then
  kdb5_util create -s -r "$REALM" -P "$(head -c 24 /dev/urandom | base64)"
  kadmin.local -q "addprinc -randkey hive/${HMS_HOST}@${REALM}"
  kadmin.local -q "addprinc -randkey client@${REALM}"
  mkdir -p "$KEYTAB_DIR"
  rm -f "$KEYTAB_DIR/hive.keytab" "$KEYTAB_DIR/client.keytab"
  kadmin.local -q "ktadd -k $KEYTAB_DIR/hive.keytab hive/${HMS_HOST}@${REALM}"
  kadmin.local -q "ktadd -k $KEYTAB_DIR/client.keytab client@${REALM}"
  # The metastore image runs as uid 1000 and the runner as an unprivileged
  # user; neither is root, so the keytabs must be world-readable. They are
  # throwaway credentials that live only for the job.
  chmod 644 "$KEYTAB_DIR"/*.keytab
  # Written last, after the chmod, so a waiter that sees it can read both
  # keytabs: the files themselves appear (root-only) before they are usable.
  touch "$KEYTAB_DIR/.ready"
  echo "realm $REALM ready; keytabs in $KEYTAB_DIR"
fi

exec krb5kdc -n
