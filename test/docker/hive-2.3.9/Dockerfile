# Hive 2.3.9 has no published apache/hive image (Docker Hub carries only
# 3.1.3 and 4.x tags), so the integration matrix's oldest supported server
# (SPEC.md §1.1, §2) is built here from the upstream binary tarballs. This
# mirrors a standalone Hive 2 metastore: Hadoop for its client libraries and
# filesystem support, and an embedded Derby metastore_db initialized once at
# build time so container start does not pay schema-init cost or need a
# writable image layer for it.
FROM eclipse-temurin:8-jre

ARG HIVE_VERSION=2.3.9
ARG HADOOP_VERSION=2.10.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# archive.apache.org is the only host for these releases and throttles
# aggressively: the same download has taken 70 s and stalled past 30 min.
# Fail fast and retry rather than hang; the workflow caches the resulting
# layers so this runs once per cache lifetime.
RUN curl -fsSL --retry 5 --retry-all-errors --retry-delay 10 --speed-limit 50000 --speed-time 60 --max-time 1200 \
      https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt \
    && curl -fsSL --retry 5 --retry-all-errors --retry-delay 10 --speed-limit 50000 --speed-time 60 --max-time 1200 \
      https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz | tar -xz -C /opt

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH=${HIVE_HOME}/bin:${HADOOP_HOME}/bin:${PATH}

# Initialize the embedded Derby metastore schema at build time so every
# container started from this image begins with a ready, empty metastore_db.
RUN ${HIVE_HOME}/bin/schematool -dbType derby -initSchema

EXPOSE 9083

# eclipse-temurin's Debian base ships bash; /dev/tcp is a bash builtin, so no
# extra netcat/curl dependency is needed just to probe the metastore's own
# port.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=12 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/9083' || exit 1

CMD ["hive", "--service", "metastore", "-p", "9083"]
