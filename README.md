# hms-client-go

A modern, pure-Go client library for **Apache Hive Metastore (HMS)** supporting **Hive 2.x, 3.x, and 4.x** with dual Binary TCP / Thrift-over-HTTP transports, Multi-Catalog support, High Availability (HA) failover, and zero Cgo dependencies.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8.svg)](go.mod)

---

## Features

* **Multi-Version Interoperability**: Seamlessly communicates with **Apache Hive 2.2+, 3.1+, and 4.0+** standalone or clustered metastores.
* **Dual Transports**:
  * **Binary TCP Thrift (`thrift://host:port`)**: Low latency, high throughput binary protocol with dynamic context deadline bindings.
  * **Thrift-over-HTTP/HTTPS (`http://` / `https://`, Hive 4.0+)**: Standard HTTP POST tunneling compatible with cloud reverse proxies (Envoy, NGINX, Knox) and JWT bearer-token authentication.
* **Multi-Catalog (Hive 3+)**: First-class support for catalog namespaces (`catName: "spark_catalog"`, `"iceberg"`, etc.).
* **High Availability (HA)**: Client-side failover across multiple HMS endpoints (`thrift://hms1:9083,thrift://hms2:9083`) with exponential backoff and connection pooling.
* **Lakehouse Table Formats**: Native metadata and SerDe builders for **Apache Iceberg**, **Delta Lake**, and **Apache Hudi**.
* **Pure Go**: Zero Cgo, no Hadoop XML or JVM runtime requirements.

---

## Documentation

* [Formal Specification (`SPEC.md`)](SPEC.md)
* [Implementation Roadmap & Tasks (`PLAN.md`)](PLAN.md)
* [Agent & Contributor Guide (`AGENTS.md`)](AGENTS.md)

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
