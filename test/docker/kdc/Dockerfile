# MIT Kerberos KDC sidecar for the Kerberized integration leg
# (.github/workflows/integration.yml, hive-4.2.1-kerberos). It creates the
# EXAMPLE.COM realm on first start, adds the metastore's service principal
# and a client principal, exports both keytabs to /keytabs for the metastore
# container and the test runner to mount, and serves KDC traffic on 88/tcp.
# apt-only, so it builds in well under a minute and is never published.
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server \
    && rm -rf /var/lib/apt/lists/*

COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY krb5-container.conf /etc/krb5.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh && echo '*/admin@EXAMPLE.COM *' > /etc/krb5kdc/kadm5.acl

EXPOSE 88/tcp
VOLUME /keytabs
ENTRYPOINT ["/entrypoint.sh"]
