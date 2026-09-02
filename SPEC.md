# `hms-client-go` Specification

> **Formal Functional, Protocol, and Interface Specification for `github.com/slachiewicz/hms-client-go`**  
> Version: 1.0.0-draft  
> Target Go Floor: **Go 1.26.0**  
> License: **Apache-2.0**

This document is the **canonical** definition of the public API, the compatibility
matrix, and the fallback rules. `PLAN.md` and `AGENTS.md` reference it and must not
restate signatures or version claims.

---

## 1. Scope and Mission

`hms-client-go` is a pure-Go client library for interacting with Apache Hive Metastore (HMS) servers. It provides:
1. Universal interoperability across **Apache Hive 2.3+, 3.1+, and 4.0+** standalone and cluster metastores.
2. Dual transport support: **Raw Binary TCP Thrift (`thrift://`)** and **Thrift-over-HTTP/HTTPS (`http://` / `https://`)**, the latter for Hive 4.0+ servers.
3. First-class support for **Hive 3+ Multi-Catalog namespaces** (`catName`).
4. Native **High Availability (HA)**, connection pooling, automatic failover, and dynamic `context.Context` deadline propagation.
5. First-class primitives for managing **Open Table Formats** (Apache Iceberg, Delta Lake, Apache Hudi) registered in Hive Metastore.

### 1.1. Out of scope for 1.0
* **Hive 2.2 and earlier**. Hive 2.2 lacks `get_table_req`, and the Hive 4 IDL this client is generated from no longer declares the legacy `get_table` / `get_table_objects_by_name` RPCs, so no fallback can be generated. Hive 2.2 is end of life; Spark 4 still defaults its metastore client to 2.3.10, which makes 2.3 the practical floor.
* **Compact protocol** (`metastore.thrift.compact.protocol.enabled=true`) and **framed transport** (`metastore.thrift.framed.transport.enabled=true`). Both are off by default on every supported server version.
* **`SkewedInfo.skewedColValueLocationMaps`**, gated on a Thrift Go release that can represent it (THRIFT-2063; fix pending upstream in PR 3778). Until that lands, the field is removed from the IDL before generation exactly as it is removed today; see Appendix A for the wire detail and §5.4 for the Go shape the client will expose once the gate clears.

Kerberos / GSSAPI is **in scope for 1.0** via a pure-Go implementation (`gokrb5`); native C Kerberos remains forbidden by the zero-Cgo invariant. See §3.1 and §5.1 (`WithKerberos`).

---

## 2. Supported Environments & Compatibility Matrix

### 2.1. RPC availability by HMS version

Verified against the `hive_metastore.thrift` IDL at tags `rel/release-2.3.9`, `rel/release-3.1.3` and `rel/release-4.2.1`. The client is generated from the 4.2.1 IDL, so an RPC must exist there to be callable at all.

| RPC | 2.3.x | 3.x | 4.x | In 4.2.1 IDL |
| :--- | :-: | :-: | :-: | :-: |
| `get_table_req`, `get_table_objects_by_name_req`, `add_partitions_req` | Y | Y | Y | Y |
| `get_database`, `get_all_databases`, `create_database`, `drop_database`, `alter_database`, `get_all_tables`, `create_table`, `alter_table`, `drop_table`, `get_partitions`, `get_partition_names`, `alter_partitions`, `drop_partition`, `get_partitions_by_names`, `get_partitions_by_filter`, `get_partition_names_ps`, `get_config_value` | Y | Y | Y | Y |
| `set_ugi` (caller identity over binary NOSASL, §3.1) | Y | Y | Y | Y |
| `get_current_notificationEventId`, `get_next_notification` (§5.7) | Y | Y | Y | Y |
| `get_table`, `get_table_objects_by_name` (legacy) | Y | Y | - | **no** |
| `get_catalogs`, `get_catalog`, `create_catalog`, `drop_catalog` | - | Y | Y | Y |
| `catName` fields on `Database`, `Table`, `Partition` and `*Request` structs | - | Y | Y | Y |
| `alter_partitions_req`, `get_partitions_req`, `get_partitions_by_names_req`, `get_partition_names_ps_req` | - | - | Y | Y |
| Thrift-over-HTTP transport (`metastore.server.thrift.transport.mode=http`) | - | - | Y | n/a |

### 2.2. Transport availability

| HMS Version Range | Binary TCP | HTTP / HTTPS |
| :--- | :-: | :-: |
| Apache Hive 2.3.0 – 2.3.10 | Y | - |
| Apache Hive 3.0.0 – 3.1.3 | Y | - |
| Apache Hive 4.0.0 – 4.2.x | Y | Y |

### 2.3. Interoperability & Client Fallback Rules

The client is generated from the Hive 4 IDL. Fields that older servers do not know are skipped by the server on read; fields that older servers do not send are left at their zero value on the client. Method-level differences are handled by the rules below. A "legacy fallback" is triggered by a `TApplicationException` of type `UNKNOWN_METHOD`; the result is cached per connection so each RPC pays the probe at most once.

* **Rule 1 (Catalog)**: A non-default catalog against a server without `get_catalogs` (Hive 2.3) returns `ErrNotSupported`. The default catalog `hive` is always accepted and is simply not written on the wire for those servers. Catalog support is probed once per connection with `get_catalogs`; `UNKNOWN_METHOD` means unsupported.
* **Rule 2 (`get_table_req`, `get_table_objects_by_name_req`, `add_partitions_req`)**: No fallback. All three exist on every supported version and the legacy forms are absent from the Hive 4 IDL.
* **Rule 3 (`alter_partitions_req`)**: On `UNKNOWN_METHOD` (Hive 2.3 and 3.x) the client degrades to `alter_partitions(dbName, tblName, parts)`.
* **Rule 4 (`get_partitions_req`)**: On `UNKNOWN_METHOD` (Hive 2.3 and 3.x) the client degrades to `get_partitions(dbName, tblName, maxParts)`.
* **Rule 5 (batching)**: `add_partitions_req` and `get_table_objects_by_name_req` are chunked (default 1000 items per request) on every version to bound request size. `add_partitions_req`'s batch size is governed by `WithPartitionBatchSize`, independent of `WithChunkSize`, which governs `get_table_objects_by_name_req` and `get_partitions_by_names_req`/`get_partitions_by_names` (Rule 6) instead — the two knobs do not share a value, so tuning one does not silently change the other's batching (§5.1).
* **Rule 6 (`get_partitions_by_names_req`)**: On `UNKNOWN_METHOD` (Hive 2.3 and 3.x) the client degrades to `get_partitions_by_names(dbName, tblName, names)`; chunked like Rule 5.
* **Rule 7 (`get_partition_names_ps_req`)**: On `UNKNOWN_METHOD` (Hive 2.3 and 3.x) the client degrades to `get_partition_names_ps(dbName, tblName, partVals, maxParts)`.

---

## 3. Wire Protocol & Transport Specification

### 3.1. Binary TCP Socket Transport (`thrift://`)
* **Scheme**: `thrift://<host>[:<port>]` (default port: `9083`).
* **Framing / Buffering**: `TBufferedTransport` (default buffer size: 8192 bytes). No `TFramedTransport`.
* **Protocol**: `TBinaryProtocol` (strict write: true, strict read: true).
* **Context propagation**:
  * `WithConnectTimeout` (§5.1) bounds connection setup -- dialing (`net.Dialer.Timeout`), the TLS handshake (when `WithTLS` is set), and the SASL handshake (when `WithPlainAuth` or `WithKerberos` is set, excepting the KDC round trips noted under `KERBEROS` below) -- as a fallback applied when the caller's `ctx` carries no deadline of its own, exactly as `WithTimeout` bounds each later per-call I/O. It defaults to `WithTimeout`'s value, so a caller who only ever tunes `WithTimeout` gets the same effective behavior as before `WithConnectTimeout` existed.
  * Binding happens in the `thrift.TClient` wrapper, `internal/transport.ContextClient`, not at the protocol layer: its `Call` is the single choke point every RPC passes through. Before delegating to the wrapped client, it sets the raw `net.Conn`'s deadline from `ctx.Deadline()`, falling back to the configured socket timeout when the context carries none, and registers `context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })` to cut that deadline short on cancellation; the stop handle is released when `Call` returns.
  * The `thrift.TSocket` handed to Thrift is constructed over a `deadlineShield` wrapping the same `net.Conn`: `SetDeadline`/`SetReadDeadline`/`SetWriteDeadline` on it are no-ops, so `TSocket`'s own per-read/write deadline resets (driven by its `TConfiguration.SocketTimeout`, left at 0) never fight `ContextClient` for ownership of the connection's deadline.
  * `ContextClient` does not exist yet during the SASL handshake (§3.1 Authentication), since it wraps the client built on top of the fully assembled transport. `DialBinary` gives the handshake the same deadline/cancel treatment directly against the raw `net.Conn` for its duration, then clears the deadline before `ContextClient` takes over.
  * A cancelled RPC returns `ctx.Err()` wrapped in `ErrUnavailable`; the connection is discarded and a new one is provisioned for the next call.
* **Authentication**:
  * `NOSASL` / `NONE`: raw binary protocol, the default. When `WithUser` (and optionally `WithUserGroups`) is set and no SASL auth is configured, the client calls `set_ugi(user, groups)` once per newly dialed connection, mirroring the Java `HiveMetaStoreClient`'s behavior under `hive.metastore.execute.setugi` (§5.1).
  * `LDAP` / `CUSTOM`: SASL `PLAIN` framing (RFC 4616 initial response `\0user\0password`), 1-byte-status/4-byte-length-prefixed negotiation frames followed by 4-byte-length-prefixed data frames (the Java `TSaslTransport` wire format). The client implements the SASL negotiation handshake in pure Go (`internal/transport/sasl.go`); see the context-propagation bullet above for how the handshake gets its deadline.
  * `KERBEROS`: SASL GSSAPI (RFC 4752) over the same socket, in the same `TSaslTransport` framing as `PLAIN`, using a pure-Go Kerberos implementation (`gokrb5`) rather than native C Kerberos, keeping the zero-Cgo invariant. Selected with `WithKerberos`; see §5.1. It is mutually exclusive with `WithPlainAuth`, and configuring both is rejected by `New` as `ErrInvalidOperation`, as are Kerberos credentials that cannot be read at all.
    * **Handshake**: three rounds -- an AP_REQ initial context token with the mutual-required option; the server's AP_REP, whose `EncAPRepPart` must echo the authenticator's timestamp (this is what proves the server holds the service key, and a server that answers `COMPLETE` before it is rejected rather than trusted); then the security-layer negotiation. When the AP_REP carries an acceptor subkey, that key -- not the ticket session key -- signs the negotiation's wrap tokens.
    * **Service principal**: `hive/<host>` for the endpoint host being dialed, matching `hive.metastore.kerberos.principal`'s default; the realm comes from `krb5.conf`'s `domain_realm` mapping. `WithKerberosServicePrincipal` overrides it, for a metastore reached through a load balancer or an alias.
    * **QOP**: `auth` only -- no security layer, so the data frames after the handshake are the plain length-prefixed frames `PLAIN` uses, with no per-frame wrapping. A server that does not offer `auth` (`hadoop.rpc.protection` set to `integrity` or `privacy`) fails the handshake rather than being silently downgraded to; the client advertises a maximum receive size of 0, since with no security layer nothing is ever wrapped.
    * **Credentials**: a keytab or a credential cache, per `WithKerberos` in §5.1. Only `FILE:` credential caches are readable; `KRB5CCNAME` naming a `DIR:`, `KEYRING:`, or `KCM:` cache is reported rather than misread as a path.
    * **Credential lifetime**: credentials are loaded once per `hms.Client`, by `New`, and shared by every connection that client dials; `Close` releases them. They are not loaded per connection: the `gokrb5` client holding them owns a session-renewal goroutine that only that release stops. A keytab or credential cache refreshed on disk afterwards is not picked up by a running client -- construct a new one.
    * **Timeouts**: `WithConnectTimeout` and the caller's `ctx` bound the SASL frames on the metastore socket, and cancelling `ctx` also abandons an in-flight KDC exchange. It does not *bound* that exchange: the AS and TGS round trips run against the KDC over `gokrb5`'s own sockets, under `krb5.conf`'s `kdc_timeout`, which is the one piece of connection setup `WithConnectTimeout` does not reach.
  * **TLS**: `WithTLS(cfg *tls.Config)` wraps the dialed socket in `tls.Client` and completes its handshake -- bound to `ctx`/`WithConnectTimeout` exactly as the SASL handshake is (see the context-propagation bullet above) -- before the SASL/binary protocol layers are attached, for a server configured with `metastore.use.SSL=true`. `ContextClient` still owns deadlines on the raw `net.Conn`, not the `tls.Conn`: `(*tls.Conn).SetDeadline` and its `Read`/`Write` counterparts delegate straight to the underlying conn, so a deadline set on the raw conn already bounds the TLS conn's I/O. See §5.1.

### 3.2. Thrift-over-HTTP/HTTPS Transport (`http://` / `https://`)
* **Availability**: Hive 4.0+ only. Connecting to a 2.x or 3.x endpoint over HTTP fails with `ErrUnavailable`: the endpoint does not speak Thrift-over-HTTP at all, so the first `TApplicationException` or non-Thrift response classifies as a transport failure, not as `ErrNotSupported` (reserved for `UNKNOWN_METHOD` and the catalog case in §2.3 Rule 1).
* **Scheme**: `http://<host>[:<port>][/path]` or `https://...`. When `path` is empty the client uses `/metastore`, the server default of `metastore.server.thrift.http.path`.
* **Framing**: HTTP POST requests carrying `TBinaryProtocol` payloads, using Thrift's `THttpClient` over a caller-supplied or default `*http.Client` (connection reuse, keep-alive, proxy resolution, HTTP/2).
* **Headers** (mirroring `HiveMetaStoreClient.createHttpClient` in Hive 4.0.1):
  * `Content-Type: application/x-thrift`
  * `Accept: application/x-thrift`
  * `User-Agent: hms-client-go/<version>` where `<version>` is taken from `debug.ReadBuildInfo`, never hardcoded.
  * Auth mode `JWT`: `Authorization: Bearer <token>`.
  * Every other auth mode: `x-actor-username: <user>`. The user defaults to the OS user when not configured.
  * Arbitrary additional headers supplied by the caller (for Knox or other reverse proxies).
* **TLS**: `WithTLS(cfg *tls.Config)` (§5.1) configures the TLS client used for `https://` endpoints, the same option used for `thrift://` (§3.1). It applies to the client this package constructs; a client supplied with `WithHTTPClient` is used as-is, so its own `Transport.TLSClientConfig` governs instead. Combining the two for an `https://` endpoint is rejected by `New` as `ErrInvalidOperation` rather than silently ignoring `WithTLS`.

---

## 4. High Availability (HA) & Clustering Specification

### 4.1. URI Parsing
The client accepts single or comma-separated endpoint URIs. All endpoints must share one scheme:
```go
uris := "thrift://hms-01.internal:9083,thrift://hms-02.internal:9083,thrift://hms-03.internal:9083"
```

### 4.2. Failover Policy
This mirrors the Java `HiveMetaStoreClient`: sticky active endpoint, not round-robin.
1. **Active endpoint**: the client connects to endpoints in list order (or random order when `WithRandomEndpointOrder()` is set) and stays on the first one that answers.
2. **Failure detection**: on `io.EOF`, `syscall.ECONNREFUSED`, `syscall.ECONNRESET`, `syscall.ETIMEDOUT`, a dial error, a context deadline caused by socket I/O, or a `TApplicationException` in the frame-desync class (`BAD_SEQUENCE_ID`, `INVALID_MESSAGE_TYPE_EXCEPTION`, `PROTOCOL_ERROR`, `WRONG_METHOD_NAME`):
   * The active endpoint is marked unhealthy with an exponential backoff cooldown (initial 1s, max 30s, full jitter).
   * A desync exception additionally means the shared connection's read/write framing is itself corrupted, not merely that one call failed, so the connection is always discarded rather than returned to its pool, regardless of point 3 below.
   * The request is retried on the next healthy endpoint, subject to point 3.
3. **Retry budget**: at most 3 attempts per RPC across endpoints by default (`WithMaxRetries`). Two distinct decisions apply:
   * A connection that could not be **acquired** at all (dial failure, or no pooled connection available) is always retried on another endpoint — this holds for every RPC, idempotent or not, since nothing has reached the server yet.
   * Once the RPC has **started** on an acquired connection, only an idempotent (read-only, `get_*`) RPC is retried elsewhere, and only on `ErrUnavailable` while the caller's context is still live: `GetCatalogs`, `GetCatalog`, `GetAllDatabases`, `GetDatabase`, `GetAllTables`, `GetTable`, `GetTables`, `GetPartitions`, `GetPartitionNames`, `GetConfigValue`, `ServerVersion`. Every other RPC (`Create*`, `Alter*`, `Drop*`, `AddPartitions`, and the ACID/lock RPCs in §5.9) returns the failure immediately once started, since the request may already have reached the server.
   * A cancelled or expired caller context never cools an endpoint (`MarkFailed` is not called) and is never itself a reason to retry: the failure is the caller's, not the endpoint's.
4. **Recovery**: a background probe (`fb303.getStatus`) re-enables cooled-down endpoints. Interval 30s. The probe runs for every client, including one constructed with a single endpoint, and on a successful probe hands the freshly dialed, already-healthy connection to that endpoint's pool rather than discarding it. The probe goroutine is cancelled by `Close` and awaited before `Close` returns.

---

## 5. API Interface Specification

Package: `hms` at the module root (`import "github.com/slachiewicz/hms-client-go"`). No generated Thrift type appears in any exported signature.

### 5.0. Catalog resolution

Every call that touches a catalog-scoped object resolves the *effective catalog* the same way, in this precedence order, highest first:

1. A per-call `InCatalog(name)` (`CatalogOption`), when passed to that call.
2. The struct's own `CatalogName` field, on a create/alter call that takes a struct: `CreateDatabase`, `CreateTable`, `AlterTable` (`newTable.CatalogName`), and `AlterDatabase` (`db.CatalogName`).
3. `WithCatalog(name)`, the client-wide default set at construction.
4. `"hive"`, the built-in default.

On a connection whose server does not support catalogs (Hive 2.3; probed once per connection via `get_catalogs`, §2.3 Rule 1): the resolved default catalog `"hive"` is never written on the wire — the request is sent exactly as it would be to a catalog-unaware server — and resolving to any other catalog returns `ErrNotSupported` without issuing the RPC.

### 5.1. Constructor and options

```go
func New(ctx context.Context, uris string, opts ...Option) (*Client, error)
func (c *Client) Close() error

func WithCatalog(name string) Option            // default catalog for every call; default "hive"
func WithTimeout(d time.Duration) Option        // socket / per-request timeout when ctx has no deadline
func WithConnectTimeout(d time.Duration) Option // dial / TLS handshake / SASL handshake timeout; defaults to WithTimeout's value
func WithMaxRetries(n int) Option
func WithRandomEndpointOrder() Option
func WithPoolSize(n int) Option
func WithChunkSize(n int) Option                // per-request chunk size for GetTables/GetPartitionsByNames; default 1000; see §5.4, §5.5
func WithPartitionBatchSize(n int) Option       // per-request batch size for AddPartitions only, independent of WithChunkSize; default 1000; see §5.5
func WithHTTPClient(hc *http.Client) Option
func WithHTTPHeaders(h map[string]string) Option
func WithBearerToken(token string) Option       // HTTP JWT mode
func WithUser(name string) Option               // x-actor-username over HTTP; set_ugi user over binary NOSASL (§3.1)
func WithUserGroups(groups ...string) Option    // set_ugi groups over binary NOSASL; repeated calls append; no effect over HTTP or non-NOSASL binary auth
func WithPlainAuth(user, password string) Option // SASL PLAIN over binary TCP
func WithKerberos(principal string, keytabOrCCache ...string) Option // SASL GSSAPI over binary TCP, pure Go (gokrb5); see §3.1
func WithKerberosServicePrincipal(spn string) Option // overrides the metastore's principal; default "hive/<host>"
func WithKrb5Config(path string) Option         // overrides KRB5_CONFIG / /etc/krb5.conf
func WithTLS(cfg *tls.Config) Option            // thrift:// (metastore.use.SSL=true) and https:// (rejected with WithHTTPClient); see §3.1, §3.2
func WithLogger(l *slog.Logger) Option          // connection lifecycle, failover, and probe events at Debug/Info; see §5.10
func WithRPCObserver(f func(RPCInfo)) Option    // per-RPC hook; see §5.10
```

`WithKerberos` resolves its arguments as follows (§3.1, `KERBEROS`):

* `principal` is the client principal, `"user"` or `"user@REALM"`; without a realm, `krb5.conf`'s `default_realm` applies. It is required with a keytab and ignored with a credential cache, whose client principal comes from the cache.
* The optional second argument names the credentials: a path ending in `.keytab` is read as a keytab, anything else as a credential cache. Further arguments are ignored.
* With no second argument, the credential cache named by `KRB5CCNAME` is used, falling back to `/tmp/krb5cc_<uid>`, so a caller who has run `kinit` need only name their principal.
* The Kerberos configuration comes from `WithKrb5Config`, else `KRB5_CONFIG`, else `/etc/krb5.conf`.

Per-call catalog override:

```go
type CatalogOption func(*catalogOpts)
func InCatalog(name string) CatalogOption
```

### 5.2. Catalog Operations (Hive 3+)

```go
type Catalog struct {
    Name        string
    Description string
    LocationURI string
}

func (c *Client) GetCatalogs(ctx context.Context) ([]string, error)
func (c *Client) GetCatalog(ctx context.Context, name string) (*Catalog, error)
func (c *Client) CreateCatalog(ctx context.Context, cat *Catalog) error
func (c *Client) DropCatalog(ctx context.Context, name string, ifExists bool) error
```

### 5.3. Database Operations

```go
type Database struct {
    CatalogName string
    Name        string
    Description string
    LocationURI string
    Parameters  map[string]string
    OwnerName   string
    OwnerType   PrincipalType
    CreateTime  time.Time // 1.0 addition
}

func (c *Client) GetAllDatabases(ctx context.Context, opts ...CatalogOption) ([]string, error)
func (c *Client) GetDatabase(ctx context.Context, name string, opts ...CatalogOption) (*Database, error)
func (c *Client) CreateDatabase(ctx context.Context, db *Database) error
func (c *Client) AlterDatabase(ctx context.Context, name string, db *Database, opts ...CatalogOption) error
func (c *Client) DropDatabase(ctx context.Context, name string, deleteData, cascade, ifExists bool, opts ...CatalogOption) error
```

An empty `Database.LocationURI` passed to `CreateDatabase` is filled in client-side before the RPC is issued; see Appendix A for the warehouse-dir resolution rule and the Hive 3.1 `get_config_value` quirk it works around. The resolved warehouse root is cached per `Client` per catalog name, so only the first such call for a given catalog pays for the `get_catalog`/`get_config_value` round trip; a warehouse directory changed on the server afterward is not picked up by a running `Client` -- construct a new one.

`Database.CreateTime` is read-only: it is populated by `GetDatabase` from the server's own timestamp, and `CreateDatabase` never writes it.

`AlterDatabase` wraps `alter_database(dbname, db)` (1.0 addition), replacing the named database's mutable properties (`Description`, `LocationURI`, `Parameters`, `OwnerName`, `OwnerType`) with `db`'s.

### 5.4. Table Operations

```go
type Table struct {
    CatalogName      string
    DatabaseName     string
    TableName        string
    Owner            string
    OwnerType        PrincipalType // 1.0 addition
    CreateTime       time.Time
    LastAccessTime   time.Time
    Retention        int32
    Storage          *StorageDescriptor
    PartitionKeys    []*FieldSchema
    Parameters       map[string]string
    ViewOriginalText string
    ViewExpandedText string
    TableType        TableType
}

func (c *Client) GetAllTables(ctx context.Context, dbName string, opts ...CatalogOption) ([]string, error)
func (c *Client) GetTable(ctx context.Context, dbName, tableName string, opts ...CatalogOption) (*Table, error)
func (c *Client) GetTables(ctx context.Context, dbName string, tableNames []string, opts ...CatalogOption) ([]*Table, error)
func (c *Client) CreateTable(ctx context.Context, table *Table) error
func (c *Client) AlterTable(ctx context.Context, dbName, tableName string, newTable *Table, opts ...CatalogOption) error
func (c *Client) DropTable(ctx context.Context, dbName, tableName string, deleteData, ifExists bool, opts ...CatalogOption) error
```

`GetTables` chunks `tableNames` into requests of at most 1000 names (default; `WithChunkSize`, §5.1). Chunking is sequential, not parallel: chunks are sent one after another, and for a mutating chunked call (`AddPartitions`, §5.5) a failure partway through leaves every earlier chunk already committed on the server.

`StorageDescriptor` gains a `Skewed *SkewedInfo` field (1.0 addition):

```go
type SkewedInfo struct {
    ColumnNames  []string
    ColumnValues [][]string
}
```

These are the wire's first two `SkewedInfo` fields (`skewedColNames`, `skewedColValues`; plain `list<string>` / `list<list<string>>`), which are not affected by the THRIFT-2063 gate in §1.1. The wire's third field, `skewedColValueLocationMaps` (`map<list<string>, string>`), remains unmodelled -- it is dropped from the generated IDL before generation entirely (§1.1, Appendix A), so the generated Thrift struct has no field for it at all, and the generated `Read` skips those bytes on the wire rather than storing them anywhere. Unlike every other unmodelled field (see "Round-trip fidelity" below), this one is genuinely lost the moment a `Table` or `Partition` is read: there is nothing for the round-trip snapshot to have captured, so it does not survive `GetTable` -> `AlterTable` either.

#### Round-trip fidelity

A `Table`, `Partition`, or `Database` returned by the server carries an internal, read-only snapshot of every field the *generated* Thrift struct carries -- not the wire's full field set, since a field the IDL generation step itself drops (today, only `SkewedInfo.skewedColValueLocationMaps`, per §1.1) was never decoded into that struct to snapshot in the first place. `AlterTable`, `AlterPartitions`, and `AlterDatabase` build the outgoing struct from that snapshot and overwrite only the modelled fields, so a field this package does not expose but the generated bindings do -- `Table.Privileges`/`RewriteEnabled`/`Id`/`TxnId`/`AccessType`/the capability lists, `Partition.Privileges`/`WriteId`/the stats fields, `StorageDescriptor.SerdeInfo`'s `Description`/`SerializerClass`/`DeserializerClass`/`SerdeType`, `Database.Privileges`/`Type`/`ConnectorName`/`RemoteDbname`/`ManagedLocationUri`, and any field a future IDL bump adds -- survives a `GetTable` -> `AlterTable` (or `GetPartitions` -> `AlterPartitions`, `GetDatabase` -> `AlterDatabase`) round trip unchanged instead of being silently reset. A `Table`, `Partition`, or `Database` built directly (a struct literal, or one of the `NewXxxTable` builders in §6) carries no snapshot and behaves exactly as before this existed. `CreateTable`, `CreateDatabase`, and `AddPartitions` always build a fresh struct and never read the snapshot, even when handed an object the server returned: they define a new object rather than preserve an existing one, so the source's server-assigned fields (`Table.Id`/`TxnId`/`WriteId`/`Privileges`, `Partition.WriteId`/`Privileges`/the stats fields, `Database.Privileges`) are left at the generated constructor's defaults instead of travelling to the new object. `AddPartitions` and `AlterPartitions` likewise take the database and table from their own arguments; a `Partition`'s own `DatabaseName`/`TableName` never override them.

### 5.5. Partition Operations

```go
type Partition struct {
    CatalogName  string
    DatabaseName string
    TableName    string
    Values       []string
    CreateTime   time.Time
    Storage      *StorageDescriptor
    Parameters   map[string]string
}

// maxParts < 0 means "all partitions". Values above math.MaxInt16 are clamped
// to the Thrift i16 limit on the wire.
func (c *Client) GetPartitions(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]*Partition, error)
func (c *Client) GetPartitionNames(ctx context.Context, dbName, tableName string, maxParts int, opts ...CatalogOption) ([]string, error)
func (c *Client) AddPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, ifNotExists bool, opts ...CatalogOption) error
func (c *Client) AlterPartitions(ctx context.Context, dbName, tableName string, partitions []*Partition, opts ...CatalogOption) error
func (c *Client) DropPartition(ctx context.Context, dbName, tableName string, partVals []string, deleteData, ifExists bool, opts ...CatalogOption) error

// 1.0 additions
func (c *Client) GetPartitionsByNames(ctx context.Context, db, tbl string, names []string, opts ...CatalogOption) ([]*Partition, error)
func (c *Client) GetPartitionsByFilter(ctx context.Context, db, tbl, filter string, maxParts int, opts ...CatalogOption) ([]*Partition, error)
func (c *Client) GetPartitionNamesByValues(ctx context.Context, db, tbl string, partialValues []string, maxParts int, opts ...CatalogOption) ([]string, error)
```

`DropPartition` with `ifExists == false` on a missing partition returns `ErrNotFound`; with `ifExists == true` it returns `nil`.

`AddPartitions` batches `partitions` the same way `GetTables` chunks `tableNames` (§5.4): at most 1000 per request by default, sent sequentially, so a failure on a later batch leaves the earlier batches already committed on the server. Its batch size is `WithPartitionBatchSize`, not `WithChunkSize`: the two are independent, so a caller tuning `WithChunkSize` for `GetTables`/`GetPartitionsByNames` does not also change `AddPartitions`' batching.

`GetPartitionsByNames` wraps `get_partitions_by_names` and is chunked like `AddPartitions`. `GetPartitionsByFilter` wraps `get_partitions_by_filter`; `filter` is Hive's partition-filter expression grammar (e.g. `"year = 2024 AND month > 6"`) and is passed through to the server verbatim — the client does not parse or validate it. `GetPartitionNamesByValues` wraps `get_partition_names_ps`, matching partitions whose leading partition-key values equal `partialValues` (a prefix; trailing keys are wildcarded).

### 5.6. Utilities

```go
func (c *Client) GetConfigValue(ctx context.Context, name, defaultValue string) (string, error)
func (c *Client) ServerVersion(ctx context.Context) (HiveVersion, error) // parsed from getVersion / get_config_value

type HiveVersion struct {
    Major int
    Minor int
    Patch int
    Raw   string // the server's literal, unparsed version string
}

func ParseHiveVersion(s string) (HiveVersion, error)
func (v HiveVersion) String() string // returns Raw
```

`ServerVersion` prefers the fb303 `getVersion` RPC. A Hive 4.x server answers with its real release (e.g. `"4.0.1"`) and that is returned as-is. A pre-4 server does not report its release there; see Appendix A for the schema-line quirk and how `ServerVersion` infers Major/Minor from it. `ParseHiveVersion` accepts both the three-component release form and the two-component schema-line form (`Patch` defaults to 0 for the latter). If `getVersion` itself is `UNKNOWN_METHOD`, `ServerVersion` falls back to the `"hive.metastore.version"` configuration value, then `"metastore.version"`, with no capability inference applied to that fallback value; it returns an error wrapping `ErrNotSupported` if the server reports a version from none of these.

### 5.7. Notifications

```go
type NotificationEvent struct {
    ID            int64
    Time          time.Time
    Type          string
    CatalogName   string
    DatabaseName  string
    TableName     string
    Message       string
    MessageFormat string
}

func (c *Client) CurrentNotificationID(ctx context.Context) (int64, error)
func (c *Client) GetNextNotifications(ctx context.Context, lastEventID int64, max int, eventTypes []string) ([]NotificationEvent, error)
```

`CurrentNotificationID` wraps `get_current_notificationEventId`. `GetNextNotifications` wraps `get_next_notification`, requesting `NotificationEventRequest{LastEvent: lastEventID}` plus `MaxEvents` (a `*int32`, set only when `max > 0`, clamped rather than wrapped) and `EventTypeList` (set only when `eventTypes` is non-empty); `eventTypes` nil or empty means every event type. Both RPCs exist on 2.3+.

`NotificationEventRequest`'s optional filter fields -- `EventTypeList`, `EventTypeSkipList`, `CatName`, `DbName`, `TableNames` -- are a Hive 4.x-only addition to the IDL: verified against `hive_metastore.thrift` at `rel/release-2.3.9` and `rel/release-3.1.3`, where `NotificationEventRequest` declares only `lastEvent` and `maxEvents`. A 2.3/3.x server's Thrift decoder silently ignores a field it does not recognize rather than rejecting the request, so `GetNextNotifications` sends `EventTypeList` unconditionally when `eventTypes` is non-empty but additionally filters the response by event type client-side, so `eventTypes` has the same effect on every supported version instead of silently becoming a no-op pre-4.0. On the response side `NotificationEvent.catName` exists from Hive 3.x (field 8 at `rel/release-3.1.3`) and is absent only on 2.3; a nil wire `CatName` maps to `NotificationEvent.CatalogName` `"hive"`.

### 5.8. Column Statistics

Read-only in 1.0; writing statistics is out of scope.

```go
func (c *Client) GetTableColumnStatistics(ctx context.Context, db, tbl string, columns []string, opts ...CatalogOption) ([]ColumnStatistics, error)

type ColumnStatistics struct {
    ColumnName string
    ColumnType string
    // Exactly one of the following is set, matching ColumnStatisticsData's
    // wire union (idl/hive_metastore.thrift): the field corresponding to
    // ColumnType's Hive category is non-nil, the rest are nil.
    Boolean   *BooleanColumnStats
    Long      *LongColumnStats
    Double    *DoubleColumnStats
    String    *StringColumnStats
    Binary    *BinaryColumnStats
    Decimal   *DecimalColumnStats
    Date      *DateColumnStats
    Timestamp *TimestampColumnStats
}

type BooleanColumnStats struct{ NumTrues, NumFalses, NumNulls int64 }
type LongColumnStats struct{ LowValue, HighValue *int64; NumNulls, NumDistinct int64 }
type DoubleColumnStats struct{ LowValue, HighValue *float64; NumNulls, NumDistinct int64 }
type StringColumnStats struct{ MaxColLen int64; AvgColLen float64; NumNulls, NumDistinct int64 }
type BinaryColumnStats struct{ MaxColLen int64; AvgColLen float64; NumNulls int64 }
type DecimalColumnStats struct{ LowValue, HighValue *Decimal; NumNulls, NumDistinct int64 }
type DateColumnStats struct{ LowValue, HighValue *time.Time; NumNulls, NumDistinct int64 }
type TimestampColumnStats struct{ LowValue, HighValue *time.Time; NumNulls, NumDistinct int64 }
type Decimal struct{ Unscaled []byte; Scale int16 }
```

`GetTableColumnStatistics` wraps `get_table_statistics_req` (`TableStatsRequest{DbName, TblName, ColNames: columns, CatName, Engine: "hive"}`, built through the generated `NewTableStatsRequest()` so the non-pointer "optional with default" fields `Engine`/`ID` keep the IDL defaults `"hive"`/`-1` rather than the Go zero value) rather than the older per-call `get_table_column_statistics`, so the whole `columns` list is fetched in one round trip. An empty `columns` returns `(nil, nil)` without issuing the RPC. Each `ColumnStatisticsObj` in the response's `statsObj` becomes one `ColumnStatistics` value, in the server's own order (not necessarily `columns`' order); a column the server has no statistics for is simply absent from the result, not an error. `ColumnStatisticsData`'s active union arm selects which of `Boolean`/`Long`/.../`Timestamp` is populated -- unlike 1.0's first draft, `TimestampColumnStatsData` (the union's 8th arm) is modelled too, since the generated `ColumnStatisticsData` already carries `TimestampStats` and skipping it would silently drop a `timestamp`-typed column's statistics. `Decimal.Unscaled` is copied so the result never aliases the wire struct; `Date`/`Timestamp` convert the wire's `daysSinceEpoch`/`secondsSinceEpoch` fields to a UTC `time.Time` (`Date` at that day's midnight); every `LowValue`/`HighValue` pointer is nil-safe, independently of its sibling. `histogram` and `bitVectors` (raw `binary` sketches) are not exposed.

`TableStatsRequest`'s fields differ across the IDL history (verified against `hive_metastore.thrift` at `rel/release-2.3.9`, `rel/release-3.1.3`, and the 4.2.1 IDL this client is generated from): 2.3.9 declares only `dbName`/`tblName`/`colNames`; 3.1.3 adds `catName`; 4.2.1 adds `validWriteIdList`/`engine`/`id`. `get_table_statistics_req` itself exists on every supported version (2.3+), so `GetTableColumnStatistics` calls it directly with no legacy fallback. On a Hive 2.3 server, the effective catalog resolves to `nil` exactly as every other catalog-scoped call resolves it (SPEC §5.0), so no `catName` field -- one that server's own IDL never declared -- is written to the wire; `engine`/`id` are likewise fields a pre-4.x server's IDL never declared, but its Thrift decoder silently skips a field it does not recognize (the same tolerance §5.7 documents for `NotificationEventRequest`), so sending the IDL defaults there is harmless.

Hive 4's metastore stores column statistics per computing engine (`engine`, above): statistics Spark, Impala, or another engine wrote under its own engine name are a separate row from Hive's, even for the same column. `GetTableColumnStatistics` always requests the `"hive"` engine's statistics (`TableStatsRequest.Engine`'s IDL default, which this call never overrides); statistics another engine computed and stored under its own name are not returned, and this call has no way to ask for them -- an `engine` option is out of scope for 1.0.

### 5.9. ACID: Locks and Transactions

Minimal surface for the metastore's lock manager and transaction lifecycle, available on Hive 2.3+.

```go
func (c *Client) OpenTransaction(ctx context.Context, user, host string) (int64, error) // open_txns, num=1
func (c *Client) CommitTransaction(ctx context.Context, txnID int64) error
func (c *Client) AbortTransaction(ctx context.Context, txnID int64) error
func (c *Client) Heartbeat(ctx context.Context, txnID int64, lockID int64) error // either id may be 0 to omit it

func (c *Client) Lock(ctx context.Context, req LockRequest) (LockResponse, error)
func (c *Client) CheckLock(ctx context.Context, lockID int64) (LockResponse, error)
func (c *Client) Unlock(ctx context.Context, lockID int64) error

type LockLevel int32
const (
    LockLevelDB        LockLevel = 1
    LockLevelTable     LockLevel = 2
    LockLevelPartition LockLevel = 3
)

type LockType int32
const (
    LockTypeSharedRead  LockType = 1
    LockTypeSharedWrite LockType = 2
    LockTypeExclusive   LockType = 3
    LockTypeExclWrite   LockType = 4
)

type LockState int32
const (
    LockStateAcquired    LockState = 1
    LockStateWaiting     LockState = 2
    LockStateAbort       LockState = 3
    LockStateNotAcquired LockState = 4
)

type LockComponent struct {
    Type      LockType
    Level     LockLevel
    Database  string
    Table     string // optional
    Partition string // optional
}

type LockRequest struct {
    Components []LockComponent
    TxnID      int64 // 0 means none
    User       string
    Host       string
}

type LockResponse struct {
    LockID       int64
    State        LockState
    ErrorMessage string
}
```

`OpenTransaction` wraps `open_txns` with `OpenTxnRequest{NumTxns: 1, User: user, Hostname: host}` and returns the single allocated `txn_ids[0]`. `Lock`, `CheckLock`, and `Unlock` wrap `lock`, `check_lock`, and `unlock` respectively; `Heartbeat` wraps `heartbeat` with `HeartbeatRequest{TxnId, LockId}` (either may be omitted by passing 0, matching the RPC's optional fields). Since 0 means "none", a negative transaction or lock id is a caller mistake and is rejected with `ErrInvalidOperation` before any RPC is issued.

`OpenTxnRequest.TxnType`, `LockRequest.ZeroWaitReadEnabled`/`ExclusiveCTAS`/`LocklessReadsEnabled`, and `LockResponse.ErrorMessage` are Hive 4.x-only wire additions, verified absent from both the 2.3.9 and 3.1.3 IDLs (`rel/release-2.3.9`/`rel/release-3.1.3` `hive_metastore.thrift` declare `OpenTxnRequest` with only `num_txns`/`user`/`hostname`/`agentInfo`, and `LockRequest`/`LockResponse` with only the fields listed in this section's Go types above). None of them has an exported equivalent; this package leaves them at their generated zero values, which a pre-4.x server's decoder never sees regardless (§2.3).

### 5.10. Observability

```go
func WithLogger(l *slog.Logger) Option
func WithRPCObserver(f func(RPCInfo)) Option

type RPCInfo struct {
    Method   string
    Endpoint string
    Attempt  int
    Duration time.Duration
    Err      error
}
```

`WithLogger` logs connection lifecycle (dial, close), failover (`MarkFailed`/`MarkHealthy` transitions), and recovery-probe events at `slog.LevelDebug` or `slog.LevelInfo`; it never logs RPC payloads or credentials. `WithRPCObserver`'s `f` is called once per attempt of every RPC (so a retried call invokes it more than once), after that attempt completes; `RPCInfo.Attempt` is 1-based and `Err` is the error `classify` would map (nil on success). An attempt that could not get a connection to the endpoint is an attempt: `f` sees it with the dial failure in `Err` and the time spent acquiring in `Duration`. `f` must not block or call back into the `Client`.

---

## 6. Lakehouse Table Format Helpers

The client MUST provide native builder helpers for registering and updating open table formats:

These conventions follow the Java builders in `xtable-hive-metastore`, the library `polytable` ports this package's builders from, rather than being invented independently -- matching them is what makes a table this client registers readable by the same Spark/Trino/Presto integrations that read one `xtable-hive-metastore` registered.

1. **Apache Iceberg**:
   * Storage Handler: `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`
   * SerDe: `org.apache.iceberg.mr.hive.HiveIcebergSerDe`
   * Input Format: `org.apache.iceberg.mr.hive.HiveIcebergInputFormat`
   * Output Format: `org.apache.iceberg.mr.hive.HiveIcebergOutputFormat`
   * Parameters: `metadata_location`, `previous_metadata_location`, `table_type: "ICEBERG"`, `iceberg.catalog: "location_based_table"`.

2. **Delta Lake**:
   * Storage Handler: `io.delta.hive.DeltaStorageHandler`; `Storage.InputFormat`/`OutputFormat` are left empty -- Delta is registered by storage handler, not an input/output format pair.
   * SerDe: `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`, with SerDe parameters `serialization.format: "1"` and `path: <location>`.
   * Parameters: `spark.sql.sources.provider: "delta"`, `table_type: "DELTA"`, `storage_handler: "io.delta.hive.DeltaStorageHandler"`.

3. **Apache Hudi**:
   * Input Format: `org.apache.hudi.hadoop.HoodieParquetInputFormat`
   * Output Format: `org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat`
   * SerDe: `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`, with the SerDe parameter `path: <location>`.
   * Parameters: `spark.sql.sources.provider: "hudi"`. `hudi.metadata-listing-enabled` is left to the caller to set.

---

## 7. Error Handling & Sentinel Errors

All HMS exceptions are unwrapped into idiomatic Go errors. The original Thrift exception message is preserved in the wrapped error text; the Thrift exception type never appears in the exported API.

| HMS Thrift Exception | Go Sentinel Error | `errors.Is` Check |
| :--- | :--- | :--- |
| `NoSuchObjectException` | `hms.ErrNotFound` | `errors.Is(err, hms.ErrNotFound)` |
| `AlreadyExistsException` | `hms.ErrAlreadyExists` | `errors.Is(err, hms.ErrAlreadyExists)` |
| `InvalidOperationException`, `InvalidObjectException`, `InvalidInputException` | `hms.ErrInvalidOperation` | `errors.Is(err, hms.ErrInvalidOperation)` |
| `MetaException` | `hms.ErrMeta` | `errors.Is(err, hms.ErrMeta)` |
| `NoSuchTxnException`, `NoSuchLockException` (§5.9) | `hms.ErrNotFound` | `errors.Is(err, hms.ErrNotFound)` |
| `TxnAbortedException`, `TxnOpenException` (§5.9) | `hms.ErrInvalidOperation` | `errors.Is(err, hms.ErrInvalidOperation)` |
| Connection / network failure, context cancellation during I/O, HTTP against a server without the HTTP transport (Hive < 4), `TApplicationException` in the frame-desync class (`BAD_SEQUENCE_ID`, `INVALID_MESSAGE_TYPE_EXCEPTION`, `PROTOCOL_ERROR`, `WRONG_METHOD_NAME`) | `hms.ErrUnavailable` | `errors.Is(err, hms.ErrUnavailable)` |
| `TApplicationException(UNKNOWN_METHOD)` with no fallback, non-default catalog against Hive 2 | `hms.ErrNotSupported` | `errors.Is(err, hms.ErrNotSupported)` |
| `ConfigValSecurityException` (`get_config_value` on a key not beginning with `hive`, `mapred`, or `hdfs`) | `hms.ErrInvalidOperation` | `errors.Is(err, hms.ErrInvalidOperation)` |

`DropDatabase` on Hive 3.1 additionally maps a bare `MetaException(java.lang.NullPointerException)` from `drop_database` to `hms.ErrNotFound`; see Appendix A.

`func Message(err error) string` extracts a generated exception's `Message` field (or a `TApplicationException`'s own text, or `err.Error()` as a last resort) without the Go struct dump the exception's default `Error()` would otherwise print; every error this package returns already uses it, so `err.Error()` is `"<op>: <message>"` with no further unwrapping needed.

---

## 8. API Stability Policy

* Module tags stay on the `v0.x` line until the `polytable` adoption described in PLAN.md's downstream note ships.
* `v1.0.0` promises standard Go-module semver for package `hms`: no breaking change to an exported identifier without a major version bump.
* `internal/` (`internal/transport`, `internal/ha`) is unstable and carries no compatibility promise at any version; it exists to be reorganized freely.
* `gen/` (the generated Thrift bindings) is not part of the API surface at any version — AGENTS.md invariant #4 already forbids a generated type from appearing in an exported `hms` identifier, so `gen/`'s own shape changing (e.g. on an IDL bump) is never a breaking change to package `hms`. It nevertheless stays importable and outside `internal/`, so test oracles and tooling may depend on `gen/hive_metastore` at a pinned module version; its generated types, fields and method names can change on any IDL regeneration without a major version bump.

---

## Appendix A. Server quirks the client compensates for

| Version | Quirk | Client behaviour |
| :--- | :--- | :--- |
| All | Go's zero value for an unset `Database.LocationURI` is written on the wire as `""`, and the generated field's default (non-optional) Thrift requiredness means the server sees that empty string rather than "absent"; the server rejects it outright (`MetaException(IllegalArgumentException: Can not create a Path from an empty string)`). | `CreateDatabase` fills in `<warehouse>/<db>.db` (lowercased db name) client-side before issuing the RPC when `LocationURI` is empty (§5.3). |
| 3.1+ | The warehouse root for the fill-in above. | Taken from the resolved catalog's own `LocationUri` (`get_catalog`): the default catalog's location *is* the warehouse dir, and a non-default catalog's location is its own warehouse root by definition. |
| 2.3 | Catalogs do not exist, so there is no catalog to ask for the warehouse root above. | Taken from the `hive.metastore.warehouse.dir` configuration value (`get_config_value`) instead. |
| 3.1 | `get_config_value` does not resolve `hive.metastore.warehouse.dir` to its `metastore.warehouse.dir` alias the way Hive 4's does; it answers empty. | Sidestepped entirely on 3.1+ by asking the catalog (row above) rather than the config value. |
| 3.1 | `drop_database` on a missing database raises a bare `MetaException(java.lang.NullPointerException)` instead of `NoSuchObjectException` (every other supported version raises the latter, which `classify` maps to `ErrNotFound`). | `DropDatabase` follows up with `get_database` on the same connection when `drop_database`'s error doesn't already classify as `ErrNotFound`; if that confirms the database is missing, the original error is replaced so the `ErrNotFound` contract holds on 3.1 too. Paid only on the error path. |
| 2.3, 3.x | `getVersion` (fb303) does not report the server's real release: every Hive 3.x release and Hive 2.3.x both answer the metastore schema line `"3.0"`. | `ServerVersion` (§5.6) tells the two apart by probing catalog support (§2.3 Rule 1) on the same connection: catalog support present → reported as `HiveVersion{Major: 3, Minor: 0}`; absent → `HiveVersion{Major: 2, Minor: 3}`. `Raw` always carries the server's literal `"3.0"` answer, so the true 3.x patch release is not recoverable from this RPC. |
| All | Several generated request/response structs declare a field with Thrift "optional with default" requiredness that this package's exported API has no equivalent for (`GetTableRequest.Engine` default `"hive"`, `GetTableRequest.ID`/`PartitionsRequest.ID`/`AlterPartitionsRequest.WriteId`/`Partition.WriteId` default `-1`, `Table.OwnerType` default `PrincipalType.USER`). A bare Go struct literal would leave these at the Go zero value instead, which the server reads as a real (and wrong) engine name, numeric id, write id, or owner type rather than "unset". | Every such request/response is built via the generated `NewXxx()` constructor (e.g. `hive_metastore.NewGetTableRequest()`, `NewTable()`, `NewPartition()`, `NewPartitionsRequest()`, `NewAlterPartitionsRequest()`) so the Thrift-declared defaults land on the wire, and only the fields this package exposes are then overwritten. |
| Pending upstream fix | `SkewedInfo.skewedColValueLocationMaps` is `map<list<string>, string>`, a list-keyed map Go cannot express; the Thrift Go generator rejects it (THRIFT-2063, fix pending in PR 3778). | `scripts/gen-thrift.sh` removes the field from the IDL before generation. Reads are unaffected: the generated code skips the unknown field with Thrift's generic `Skip`, which handles list-typed keys. The client never writes it. See §1.1 and §5.4. |
