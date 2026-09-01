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
* **Kerberos / GSSAPI**. Native C Kerberos is forbidden by the zero-Cgo invariant. A pure-Go implementation (for example `gokrb5`) is acceptable in a later release but is not part of 1.0.
* **Compact protocol** (`metastore.thrift.compact.protocol.enabled=true`) and **framed transport** (`metastore.thrift.framed.transport.enabled=true`). Both are off by default on every supported server version.
* **`SkewedInfo.skewedColValueLocationMaps`**. Its Thrift type is `map<list<string>, string>`, which Go cannot express and the Thrift Go generator rejects (THRIFT-2063). The field is removed from the IDL before generation. Reads are unaffected: the generated code skips the unknown field with Thrift's generic `Skip`, which handles list-typed keys. The client never writes it. Skewed column names and values (fields 1 and 2) are fully supported.

---

## 2. Supported Environments & Compatibility Matrix

### 2.1. RPC availability by HMS version

Verified against the `hive_metastore.thrift` IDL at tags `rel/release-2.3.9`, `rel/release-3.1.3` and `rel/release-4.0.1`. The client is generated from the 4.0.1 IDL, so an RPC must exist there to be callable at all.

| RPC | 2.3.x | 3.x | 4.x | In 4.0.1 IDL |
| :--- | :-: | :-: | :-: | :-: |
| `get_table_req`, `get_table_objects_by_name_req`, `add_partitions_req` | Y | Y | Y | Y |
| `get_database`, `get_all_databases`, `create_database`, `drop_database`, `get_all_tables`, `create_table`, `alter_table`, `drop_table`, `get_partitions`, `get_partition_names`, `alter_partitions`, `drop_partition`, `get_config_value` | Y | Y | Y | Y |
| `get_table`, `get_table_objects_by_name` (legacy) | Y | Y | - | **no** |
| `get_catalogs`, `get_catalog`, `create_catalog`, `drop_catalog` | - | Y | Y | Y |
| `catName` fields on `Database`, `Table`, `Partition` and `*Request` structs | - | Y | Y | Y |
| `alter_partitions_req`, `get_partitions_req` | - | - | Y | Y |
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
* **Rule 5 (batching)**: `add_partitions_req` and `get_table_objects_by_name_req` are chunked (default 1000 items per request) on every version to bound request size.

---

## 3. Wire Protocol & Transport Specification

### 3.1. Binary TCP Socket Transport (`thrift://`)
* **Scheme**: `thrift://<host>[:<port>]` (default port: `9083`).
* **Framing / Buffering**: `TBufferedTransport` (default buffer size: 8192 bytes). No `TFramedTransport`.
* **Protocol**: `TBinaryProtocol` (strict write: true, strict read: true).
* **Context propagation**:
  * `thrift.TTransport.Read` and `Write` receive no context. Since Thrift 0.14 every `thrift.TProtocol` method receives one, so the client binds the request context at the **protocol layer**: before each RPC the socket is handed the request context and derives `net.Conn.SetReadDeadline` / `SetWriteDeadline` from `ctx.Deadline()`, falling back to the configured socket timeout when the context has no deadline.
  * Cancellation is enforced with `context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })`, registered per RPC and released on return. A cancelled RPC returns `ctx.Err()` wrapped in `ErrUnavailable`; the connection is discarded and a new one is provisioned for the next call.
* **Authentication**:
  * `NOSASL` / `NONE`: raw binary protocol, the default.
  * `LDAP` / `CUSTOM`: SASL `PLAIN` framing (`TSaslClientTransport`, RFC 4616 initial response `\0user\0password`) over the same socket. The client implements the SASL negotiation handshake in pure Go.
  * `KERBEROS`: out of scope for 1.0 (see §1.1).

### 3.2. Thrift-over-HTTP/HTTPS Transport (`http://` / `https://`)
* **Availability**: Hive 4.0+ only. Connecting to a 2.x or 3.x endpoint over HTTP fails with `ErrNotSupported` after the first `TApplicationException` or non-Thrift response.
* **Scheme**: `http://<host>[:<port>][/path]` or `https://...`. When `path` is empty the client uses `/metastore`, the server default of `metastore.server.thrift.http.path`.
* **Framing**: HTTP POST requests carrying `TBinaryProtocol` payloads, using Thrift's `THttpClient` over a caller-supplied or default `*http.Client` (connection reuse, keep-alive, proxy resolution, HTTP/2).
* **Headers** (mirroring `HiveMetaStoreClient.createHttpClient` in Hive 4.0.1):
  * `Content-Type: application/x-thrift`
  * `Accept: application/x-thrift`
  * `User-Agent: hms-client-go/<version>` where `<version>` is taken from `debug.ReadBuildInfo`, never hardcoded.
  * Auth mode `JWT`: `Authorization: Bearer <token>`.
  * Every other auth mode: `x-actor-username: <user>`. The user defaults to the OS user when not configured.
  * Arbitrary additional headers supplied by the caller (for Knox or other reverse proxies).

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
2. **Failure detection**: on `io.EOF`, `syscall.ECONNREFUSED`, `syscall.ECONNRESET`, `syscall.ETIMEDOUT`, a dial error, or a context deadline caused by socket I/O:
   * The active endpoint is marked unhealthy with an exponential backoff cooldown (initial 1s, max 30s, full jitter).
   * The request is retried on the next healthy endpoint.
3. **Retry budget**: at most 3 attempts per RPC across endpoints by default (`WithMaxRetries`). Non-idempotent RPCs (`create_*`, `add_partitions*`, `drop_*`) are retried only when the failure happened before the request was flushed.
4. **Recovery**: a background probe (`fb303.getStatus`) re-enables cooled-down endpoints. Interval 30s, cancelled by `Close`.

---

## 5. API Interface Specification

Package: `hms` at the module root (`import "github.com/slachiewicz/hms-client-go"`). No generated Thrift type appears in any exported signature.

### 5.1. Constructor and options

```go
func New(ctx context.Context, uris string, opts ...Option) (*Client, error)
func (c *Client) Close() error

func WithCatalog(name string) Option            // default catalog for every call; default "hive"
func WithTimeout(d time.Duration) Option        // socket / per-request timeout when ctx has no deadline
func WithMaxRetries(n int) Option
func WithRandomEndpointOrder() Option
func WithPoolSize(n int) Option
func WithHTTPClient(hc *http.Client) Option
func WithHTTPHeaders(h map[string]string) Option
func WithBearerToken(token string) Option       // HTTP JWT mode
func WithUser(name string) Option               // x-actor-username / SASL PLAIN user
func WithPlainAuth(user, password string) Option // SASL PLAIN over binary TCP
```

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
}

func (c *Client) GetAllDatabases(ctx context.Context, opts ...CatalogOption) ([]string, error)
func (c *Client) GetDatabase(ctx context.Context, name string, opts ...CatalogOption) (*Database, error)
func (c *Client) CreateDatabase(ctx context.Context, db *Database) error
func (c *Client) DropDatabase(ctx context.Context, name string, deleteData, cascade, ifExists bool, opts ...CatalogOption) error
```

### 5.4. Table Operations

```go
type Table struct {
    CatalogName      string
    DatabaseName     string
    TableName        string
    Owner            string
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

`GetTables` chunks `tableNames` into requests of at most 1000 names.

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
```

`DropPartition` with `ifExists == false` on a missing partition returns `ErrNotFound`; with `ifExists == true` it returns `nil`.

### 5.6. Utilities

```go
func (c *Client) GetConfigValue(ctx context.Context, name, defaultValue string) (string, error)
func (c *Client) ServerVersion(ctx context.Context) (HiveVersion, error) // parsed from getVersion / get_config_value
```

---

## 6. Lakehouse Table Format Helpers

The client MUST provide native builder helpers for registering and updating open table formats:

1. **Apache Iceberg**:
   * Storage Handler: `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`
   * SerDe: `org.apache.iceberg.mr.hive.HiveIcebergSerDe`
   * Input Format: `org.apache.iceberg.mr.hive.HiveIcebergInputFormat`
   * Output Format: `org.apache.iceberg.mr.hive.HiveIcebergOutputFormat`
   * Parameters: `metadata_location`, `previous_metadata_location`, `table_type: "ICEBERG"`.

2. **Delta Lake**:
   * Input Format: `io.delta.hive.DeltaInputFormat`
   * Output Format: `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat`
   * SerDe: `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`
   * Parameters: `spark.sql.sources.provider: "delta"`, `table_type: "DELTA"`.

3. **Apache Hudi**:
   * Input Format: `org.apache.hudi.hadoop.HoodieParquetInputFormat`
   * Output Format: `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat`
   * SerDe: `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`
   * Parameters: `spark.sql.sources.provider: "hudi"`.

---

## 7. Error Handling & Sentinel Errors

All HMS exceptions are unwrapped into idiomatic Go errors. The original Thrift exception message is preserved in the wrapped error text; the Thrift exception type never appears in the exported API.

| HMS Thrift Exception | Go Sentinel Error | `errors.Is` Check |
| :--- | :--- | :--- |
| `NoSuchObjectException` | `hms.ErrNotFound` | `errors.Is(err, hms.ErrNotFound)` |
| `AlreadyExistsException` | `hms.ErrAlreadyExists` | `errors.Is(err, hms.ErrAlreadyExists)` |
| `InvalidOperationException`, `InvalidObjectException`, `InvalidInputException` | `hms.ErrInvalidOperation` | `errors.Is(err, hms.ErrInvalidOperation)` |
| `MetaException` | `hms.ErrMeta` | `errors.Is(err, hms.ErrMeta)` |
| Connection / network failure, context cancellation during I/O | `hms.ErrUnavailable` | `errors.Is(err, hms.ErrUnavailable)` |
| `TApplicationException(UNKNOWN_METHOD)` with no fallback, HTTP against Hive < 4, non-default catalog against Hive 2 | `hms.ErrNotSupported` | `errors.Is(err, hms.ErrNotSupported)` |
