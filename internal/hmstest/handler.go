// Package hmstest is an in-process fake Hive Metastore Thrift server used
// by this module's own tests. It is not part of the public API.
package hmstest

import (
	"context"
	"math"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/slachiewicz/hms-client-go/gen/fb303"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// now32 returns the current Unix time clamped to fit the int32 CreateTime
// fields used throughout the generated Thrift types.
func now32() int32 {
	u := time.Now().Unix()
	if u > math.MaxInt32 {
		u = math.MaxInt32
	}
	return int32(u)
}

// Version selects which Hive Metastore version the fake server emulates.
// The version determines which RPCs exist on the wire and whether a
// non-nil CatName is accepted (see removedRPCs and the cat helper).
type Version int

// Supported fake-server versions, oldest first.
const (
	// Hive23 emulates Hive 2.3: no catalog support, no PartitionsRequest
	// or AlterPartitionsRequest RPCs.
	Hive23 Version = iota
	// Hive31 emulates Hive 3.1: catalog support, but still no
	// PartitionsRequest or AlterPartitionsRequest RPCs.
	Hive31
	// Hive40 emulates Hive 4.0: every RPC used by this client exists.
	Hive40
)

// Store is the fake server's in-memory state. All fields are exported so
// tests can seed or inspect it directly; mu guards concurrent access from
// RPC handlers running on different connections.
type Store struct {
	mu sync.Mutex

	// Catalogs holds catalogs keyed by name.
	Catalogs map[string]*hive_metastore.Catalog
	// Databases holds databases keyed by "cat.db".
	Databases map[string]*hive_metastore.Database
	// Tables holds tables keyed by "cat.db.tbl".
	Tables map[string]*hive_metastore.Table
	// Partitions holds partitions keyed by "cat.db.tbl".
	Partitions map[string][]*hive_metastore.Partition
	// ColumnStats holds column statistics keyed by "cat.db.tbl", the same
	// key shape as Tables/Partitions, served (filtered to the request's
	// ColNames) by GetTableStatisticsReq. Seeded via Server.SeedColumnStats
	// -- this fake server implements no write path for statistics, mirroring
	// this package's own read-only GetTableColumnStatistics (SPEC §5.8).
	ColumnStats map[string][]*hive_metastore.ColumnStatisticsObj
	// Config holds metastore configuration values served by GetConfigValue.
	Config map[string]string
	// Events holds the notification event log, oldest first, appended to
	// by CreateDatabase/DropDatabase/CreateTable/AlterTable/DropTable/
	// AddPartitionsReq/DropPartition (see recordEvent) and served by
	// GetNextNotification/GetCurrentNotificationEventId.
	Events []*hive_metastore.NotificationEvent
	// Acid holds the transaction/lock table served by
	// OpenTxns/CommitTxn/AbortTxn/Heartbeat/Lock/CheckLock/Unlock (SPEC
	// §5.9), defined in acid.go alongside those handler methods. It has
	// its own mutex (see acidState's doc comment) rather than sharing mu
	// above.
	Acid *acidState
}

// NewStore returns an empty Store pre-populated with the default "hive"
// catalog, matching a freshly installed metastore.
func NewStore() *Store {
	return &Store{
		Catalogs: map[string]*hive_metastore.Catalog{
			"hive": {Name: "hive", LocationUri: "/user/hive/warehouse"},
		},
		Databases:   map[string]*hive_metastore.Database{},
		Tables:      map[string]*hive_metastore.Table{},
		Partitions:  map[string][]*hive_metastore.Partition{},
		ColumnStats: map[string][]*hive_metastore.ColumnStatisticsObj{},
		Config:      map[string]string{},
		Acid:        newAcidState(),
	}
}

// recordEvent appends a notification event to the store's event log
// (Store.Events), mirroring what a real Hive Metastore's
// DbNotificationListener records for the create/alter/drop RPCs this fake
// server implements (SPEC §5.7). Event IDs are assigned monotonically from
// 1. db is always set on the wire NotificationEvent; tbl is only set (and
// only appears in Message) for a table/partition-level event -- a
// database-level event (CREATE_DATABASE, DROP_DATABASE) passes tbl="".
// Message is compact JSON, {"db":"<db>"} or {"db":"<db>","table":"<tbl>"},
// matching the shape a real metastore's own DbNotificationListener message
// bodies carry (this fake server implements no other part of that
// encoding). The store's mu must already be held by the caller.
func (s *Store) recordEvent(eventType, db, tbl string) {
	msg := `{"db":"` + db + `"`
	if tbl != "" {
		msg += `,"table":"` + tbl + `"`
	}
	msg += `}`
	format := "json-0.2"
	ev := &hive_metastore.NotificationEvent{
		EventId:       int64(len(s.Events)) + 1,
		EventTime:     now32(),
		EventType:     eventType,
		DbName:        &db,
		Message:       msg,
		MessageFormat: &format,
	}
	if tbl != "" {
		ev.TableName = &tbl
	}
	s.Events = append(s.Events, ev)
}

// recorder tracks RPCs invoked against the fake server, in call order,
// along with the most recently observed argument value per method name.
// It is safe for concurrent use.
type recorder struct {
	mu    sync.Mutex
	calls []string
	last  map[string]any
}

func (r *recorder) record(name string, args any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
	if r.last == nil {
		r.last = make(map[string]any)
	}
	r.last[name] = args
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recorder) lastArgs(name string) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[name]
}

// handler implements the subset of hive_metastore.ThriftHiveMetastore
// (and, via GetStatus, fb303.FacebookService) that the client library
// exercises. Every other method is left to the embedded nil interface;
// those RPCs are either deleted from the processor map for the emulated
// version or are never called by tests.
type handler struct {
	hive_metastore.ThriftHiveMetastore
	v     Version
	store *Store
	rec   *recorder
}

// cat resolves an optional CatName as sent on the wire by RPCs that carry
// an explicit catalog field. A nil CatName means the default "hive"
// catalog. Hive23 predates catalog support, so a non-nil CatName there is
// itself the bug under test: it means the client wrote a field the real
// 2.3 server would not understand.
func cat(v Version, catName *string) (string, error) {
	if catName == nil {
		return "hive", nil
	}
	if v == Hive23 {
		return "", &hive_metastore.MetaException{Message: "unexpected catName"}
	}
	return *catName, nil
}

// splitCatDB parses a database name that may carry a catalog qualifier of
// the form "@<cat>#<db>", as used by RPCs with no dedicated CatName field.
// A plain name is treated as belonging to the "hive" catalog.
func splitCatDB(name string) (catName, db string) {
	rest, ok := strings.CutPrefix(name, "@")
	if !ok {
		return "hive", name
	}
	c, d, ok := strings.Cut(rest, "#")
	if !ok {
		return "hive", name
	}
	return c, d
}

// resolveDB applies splitCatDB and rejects a catalog qualifier on Hive23,
// which predates catalog support.
func resolveDB(v Version, name string) (catName, db string, err error) {
	if v == Hive23 && strings.HasPrefix(name, "@") {
		return "", "", &hive_metastore.MetaException{Message: "unexpected catalog qualifier"}
	}
	c, d := splitCatDB(name)
	return c, d, nil
}

func dbKey(catName, db string) string {
	return catName + "." + db
}

func tblKey(catName, db, tbl string) string {
	return catName + "." + db + "." + tbl
}

func findPartition(parts []*hive_metastore.Partition, values []string) int {
	for i, p := range parts {
		if slices.Equal(p.Values, values) {
			return i
		}
	}
	return -1
}

func truncateParts(parts []*hive_metastore.Partition, maxParts int16) []*hive_metastore.Partition {
	n := len(parts)
	if maxParts >= 0 && int(maxParts) < n {
		n = int(maxParts)
	}
	out := make([]*hive_metastore.Partition, n)
	copy(out, parts[:n])
	return out
}

// partitionsMatchingNames returns, in parts order, the partitions of tbl
// whose computed name (partitionName) is in names.
func partitionsMatchingNames(tbl *hive_metastore.Table, parts []*hive_metastore.Partition, names []string) []*hive_metastore.Partition {
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	var out []*hive_metastore.Partition
	for _, p := range parts {
		if want[partitionName(tbl, p)] {
			out = append(out, p)
		}
	}
	return out
}

// partitionMatchesPartialValues reports whether p's Values match
// partialValues as a prefix, treating an empty string in partialValues as
// a wildcard for that position (get_partition_names_ps's "partial values"
// semantics: unset trailing -- or interior -- key positions match any
// value).
func partitionMatchesPartialValues(p *hive_metastore.Partition, partialValues []string) bool {
	for i, v := range partialValues {
		if v == "" {
			continue
		}
		if i >= len(p.Values) || p.Values[i] != v {
			return false
		}
	}
	return true
}

// partitionsMatchingPartialValues returns, in parts order, the partitions
// matching partialValues (see partitionMatchesPartialValues).
func partitionsMatchingPartialValues(parts []*hive_metastore.Partition, partialValues []string) []*hive_metastore.Partition {
	var out []*hive_metastore.Partition
	for _, p := range parts {
		if partitionMatchesPartialValues(p, partialValues) {
			out = append(out, p)
		}
	}
	return out
}

// parsePartitionFilter parses the fake server's supported subset of Hive's
// partition-filter expression grammar (SPEC §5.5): one or more
// "key = 'value'" equality terms joined by "and" (case-insensitive), e.g.
// "dt = '2024-01-01' and region = 'eu'". This fake server implements no
// general expression grammar (no OR, no other operators, no unquoted or
// numeric literals); anything outside that subset is rejected with a
// MetaException, mirroring how a real metastore reports a filter it
// cannot parse. An empty filter matches every partition.
func parsePartitionFilter(filter string) (map[string]string, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return nil, nil
	}
	terms := filterAndSplit.Split(filter, -1)
	out := make(map[string]string, len(terms))
	for _, term := range terms {
		key, val, ok := parseFilterEqTerm(term)
		if !ok {
			return nil, &hive_metastore.MetaException{Message: "unsupported partition filter (only \"key = 'value'\" terms joined by \"and\" are supported): " + filter}
		}
		out[key] = val
	}
	return out, nil
}

// filterAndSplit splits a partition filter on "and", case-insensitively,
// used by parsePartitionFilter.
var filterAndSplit = regexp.MustCompile(`(?i)\s+and\s+`)

// parseFilterEqTerm parses one "key = 'value'" term, as used by
// parsePartitionFilter. It reports ok=false for anything else, including
// an unquoted or missing value.
func parseFilterEqTerm(term string) (key, val string, ok bool) {
	idx := strings.Index(term, "=")
	if idx < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(term[:idx])
	raw := strings.TrimSpace(term[idx+1:])
	if key == "" || len(raw) < 2 || raw[0] != '\'' || raw[len(raw)-1] != '\'' {
		return "", "", false
	}
	inner := raw[1 : len(raw)-1]
	if strings.Contains(inner, "'") {
		// A quote character inside the value means raw isn't actually one
		// quoted literal, e.g. an "or"-joined term this fake server
		// doesn't support ("dt = '1' or region = 'us'") that happens to
		// both start and end with a single quote.
		return "", "", false
	}
	return key, inner, true
}

// partitionMatchesFilterTerms reports whether p satisfies every key=value
// term in terms, resolving each key against tbl.PartitionKeys.
func partitionMatchesFilterTerms(tbl *hive_metastore.Table, p *hive_metastore.Partition, terms map[string]string) bool {
	for k, v := range terms {
		idx := -1
		for i, fk := range tbl.PartitionKeys {
			if fk.Name == k {
				idx = i
				break
			}
		}
		if idx < 0 || idx >= len(p.Values) || p.Values[idx] != v {
			return false
		}
	}
	return true
}

func partitionName(tbl *hive_metastore.Table, p *hive_metastore.Partition) string {
	segs := make([]string, len(tbl.PartitionKeys))
	for i, k := range tbl.PartitionKeys {
		v := ""
		if i < len(p.Values) {
			v = p.Values[i]
		}
		segs[i] = k.Name + "=" + v
	}
	return strings.Join(segs, "/")
}

// GetConfigValueArgs is the recorded LastArgs value for GetConfigValue.
type GetConfigValueArgs struct {
	Name         string
	DefaultValue string
}

// DropDatabaseArgs is the recorded LastArgs value for DropDatabase.
type DropDatabaseArgs struct {
	Name       string
	DeleteData bool
	Cascade    bool
}

// AlterTableArgs is the recorded LastArgs value for AlterTable.
type AlterTableArgs struct {
	DbName  string
	TblName string
	NewTbl  *hive_metastore.Table
}

// DropTableArgs is the recorded LastArgs value for DropTable.
type DropTableArgs struct {
	DbName     string
	TableName  string
	DeleteData bool
}

// GetPartitionsArgs is the recorded LastArgs value for GetPartitions.
type GetPartitionsArgs struct {
	DbName   string
	TblName  string
	MaxParts int16
}

// GetPartitionNamesArgs is the recorded LastArgs value for GetPartitionNames.
type GetPartitionNamesArgs struct {
	DbName   string
	TblName  string
	MaxParts int16
}

// AlterPartitionsArgs is the recorded LastArgs value for AlterPartitions.
type AlterPartitionsArgs struct {
	DbName  string
	TblName string
	Parts   []*hive_metastore.Partition
}

// DropPartitionArgs is the recorded LastArgs value for DropPartition.
type DropPartitionArgs struct {
	DbName     string
	TblName    string
	PartVals   []string
	DeleteData bool
}

// GetPartitionsByNamesArgs is the recorded LastArgs value for
// GetPartitionsByNames.
type GetPartitionsByNamesArgs struct {
	DbName  string
	TblName string
	Names   []string
}

// GetPartitionsByFilterArgs is the recorded LastArgs value for
// GetPartitionsByFilter.
type GetPartitionsByFilterArgs struct {
	DbName   string
	TblName  string
	Filter   string
	MaxParts int16
}

// GetPartitionNamesPsArgs is the recorded LastArgs value for
// GetPartitionNamesPs.
type GetPartitionNamesPsArgs struct {
	DbName   string
	TblName  string
	PartVals []string
	MaxParts int16
}

// AlterDatabaseArgs is the recorded LastArgs value for AlterDatabase.
type AlterDatabaseArgs struct {
	Name string
	Db   *hive_metastore.Database
}

// SetUgiArgs is the recorded LastArgs value for SetUgi.
type SetUgiArgs struct {
	User   string
	Groups []string
}

// SetUgi records the caller's identity and echoes groupNames back, the way
// a real metastore's set_ugi RPC does.
func (h *handler) SetUgi(_ context.Context, userName string, groupNames []string) ([]string, error) {
	h.rec.record("set_ugi", SetUgiArgs{User: userName, Groups: groupNames})
	return groupNames, nil
}

// GetStatus reports the fb303 service as always alive.
func (h *handler) GetStatus(_ context.Context) (fb303.FbStatus, error) {
	h.rec.record("getStatus", nil)
	return fb303.FbStatus_ALIVE, nil
}

// GetVersion reports the emulated Hive Metastore version string (e.g.
// "4.0.1" on Hive40, "3.0" on Hive23 and Hive31), the same value
// versionString computes. It is the fb303 RPC (*Client).ServerVersion
// tries first, ahead of the "hive.metastore.version"/"metastore.version"
// get_config_value fallback.
func (h *handler) GetVersion(_ context.Context) (string, error) {
	h.rec.record("getVersion", nil)
	return versionString(h.v), nil
}

// GetConfigValue returns the stored config value for name, or defaultValue
// when unset.
func (h *handler) GetConfigValue(_ context.Context, name, defaultValue string) (string, error) {
	h.rec.record("get_config_value", GetConfigValueArgs{Name: name, DefaultValue: defaultValue})
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if v, ok := h.store.Config[name]; ok {
		return v, nil
	}
	return defaultValue, nil
}

// GetCatalogs lists all catalog names.
func (h *handler) GetCatalogs(_ context.Context) (*hive_metastore.GetCatalogsResponse, error) {
	h.rec.record("get_catalogs", nil)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	names := make([]string, 0, len(h.store.Catalogs))
	for name := range h.store.Catalogs {
		names = append(names, name)
	}
	slices.Sort(names)
	return &hive_metastore.GetCatalogsResponse{Names: names}, nil
}

// GetCatalog returns one catalog by name.
func (h *handler) GetCatalog(_ context.Context, req *hive_metastore.GetCatalogRequest) (*hive_metastore.GetCatalogResponse, error) {
	h.rec.record("get_catalog", req)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	c, ok := h.store.Catalogs[req.Name]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "catalog " + req.Name + " not found"}
	}
	return &hive_metastore.GetCatalogResponse{Catalog: c}, nil
}

// CreateCatalog adds a new catalog.
func (h *handler) CreateCatalog(_ context.Context, req *hive_metastore.CreateCatalogRequest) error {
	h.rec.record("create_catalog", req)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	name := req.Catalog.Name
	if _, ok := h.store.Catalogs[name]; ok {
		return &hive_metastore.AlreadyExistsException{Message: "catalog " + name + " already exists"}
	}
	stored := *req.Catalog
	if stored.CreateTime == nil || *stored.CreateTime == 0 {
		t := now32()
		stored.CreateTime = &t
	}
	h.store.Catalogs[name] = &stored
	return nil
}

// DropCatalog removes a catalog by name.
func (h *handler) DropCatalog(_ context.Context, req *hive_metastore.DropCatalogRequest) error {
	h.rec.record("drop_catalog", req)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if _, ok := h.store.Catalogs[req.Name]; !ok {
		return &hive_metastore.NoSuchObjectException{Message: "catalog " + req.Name + " not found"}
	}
	delete(h.store.Catalogs, req.Name)
	return nil
}

// GetAllDatabases lists database names in the default "hive" catalog.
func (h *handler) GetAllDatabases(_ context.Context) ([]string, error) {
	h.rec.record("get_all_databases", nil)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	prefix := "hive."
	var names []string
	for key, db := range h.store.Databases {
		if strings.HasPrefix(key, prefix) {
			names = append(names, db.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// GetDatabases lists database names in one catalog matching a glob
// pattern. The client calls this (instead of get_all_databases) when a
// non-default catalog is requested, since get_all_databases has no
// catalog parameter: pattern carries the same "@<cat>#<glob>" qualifier
// convention as get_database/drop_database (see splitCatDB / resolveDB),
// and is rejected on Hive23 for the same reason resolveDB rejects it
// elsewhere (Hive23 predates catalogs, so the client never emits the
// qualifier for it). An empty or "*" glob matches every database in the
// resolved catalog.
func (h *handler) GetDatabases(_ context.Context, pattern string) ([]string, error) {
	h.rec.record("get_databases", pattern)
	catName, glob, err := resolveDB(h.v, pattern)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	prefix := catName + "."
	var names []string
	for key, db := range h.store.Databases {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if glob == "" || glob == "*" {
			names = append(names, db.Name)
			continue
		}
		if ok, _ := path.Match(glob, db.Name); ok {
			names = append(names, db.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// GetDatabase returns one database by (possibly catalog-qualified) name.
func (h *handler) GetDatabase(_ context.Context, name string) (*hive_metastore.Database, error) {
	h.rec.record("get_database", name)
	catName, db, err := resolveDB(h.v, name)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	d, ok := h.store.Databases[dbKey(catName, db)]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "database " + db + " not found"}
	}
	return d, nil
}

// CreateDatabase adds a new database. Database.Name may carry a catalog
// qualifier, in which case the stored Database.Name is the unqualified
// form so later lookups by plain name succeed.
func (h *handler) CreateDatabase(_ context.Context, db *hive_metastore.Database) error {
	h.rec.record("create_database", db)
	// A real Hive Metastore rejects an empty locationUri outright rather
	// than computing a warehouse-relative default itself: Hive's DDL path
	// (not the metastore) fills that default before the RPC is even
	// issued, so the fake server mirrors the exact MetaException the
	// client's own CreateDatabase (client.go) exists to avoid triggering.
	if db.LocationUri == "" {
		return &hive_metastore.MetaException{Message: "java.lang.IllegalArgumentException: Can not create a Path from an empty string"}
	}
	catName, name, err := resolveDB(h.v, db.Name)
	if err != nil {
		return err
	}
	// Unlike get_database/drop_database, create_database's wire struct
	// carries a real CatalogName field (set on Hive31/40, see
	// (*Client).resolveCat); it takes precedence over the "@cat#" prefix
	// convention db.Name never actually carries here, so a database
	// created in a non-default catalog is stored under that catalog.
	if db.CatalogName != nil && *db.CatalogName != "" {
		catName = *db.CatalogName
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := dbKey(catName, name)
	if _, ok := h.store.Databases[key]; ok {
		return &hive_metastore.AlreadyExistsException{Message: "database " + name + " already exists"}
	}
	stored := *db
	stored.Name = name
	if stored.CreateTime == nil || *stored.CreateTime == 0 {
		t := now32()
		stored.CreateTime = &t
	}
	h.store.Databases[key] = &stored
	h.store.recordEvent("CREATE_DATABASE", name, "")
	return nil
}

// AlterDatabase replaces a database's mutable properties (Description,
// LocationURI, Parameters, OwnerName, OwnerType) with db's, keyed by the
// existing (possibly catalog-qualified) name.
func (h *handler) AlterDatabase(_ context.Context, name string, db *hive_metastore.Database) error {
	h.rec.record("alter_database", AlterDatabaseArgs{Name: name, Db: db})
	catName, dbName, err := resolveDB(h.v, name)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := dbKey(catName, dbName)
	if _, ok := h.store.Databases[key]; !ok {
		return &hive_metastore.NoSuchObjectException{Message: "database " + dbName + " not found"}
	}
	stored := *db
	stored.Name = dbName
	h.store.Databases[key] = &stored
	return nil
}

// DropDatabase removes a database, refusing when it still has tables and
// cascade is false.
func (h *handler) DropDatabase(_ context.Context, name string, deleteData, cascade bool) error {
	h.rec.record("drop_database", DropDatabaseArgs{Name: name, DeleteData: deleteData, Cascade: cascade})
	catName, db, err := resolveDB(h.v, name)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := dbKey(catName, db)
	if _, ok := h.store.Databases[key]; !ok {
		// A real Hive 3.1.3 metastore NPEs instead of raising
		// NoSuchObjectException when drop_database targets a database
		// that does not exist, wrapping the NPE in a bare MetaException;
		// Hive 2.3 and 4.x both raise NoSuchObjectException as documented.
		// (*Client).DropDatabase copes with this by following up with
		// get_database on the same connection when drop_database's error
		// isn't already ErrNotFound.
		if h.v == Hive31 {
			return &hive_metastore.MetaException{Message: "java.lang.NullPointerException"}
		}
		return &hive_metastore.NoSuchObjectException{Message: "database " + db + " not found"}
	}
	prefix := key + "."
	var tblKeys []string
	for tk := range h.store.Tables {
		if strings.HasPrefix(tk, prefix) {
			tblKeys = append(tblKeys, tk)
		}
	}
	if len(tblKeys) > 0 && !cascade {
		return &hive_metastore.InvalidOperationException{Message: "database " + db + " is not empty"}
	}
	for _, tk := range tblKeys {
		delete(h.store.Tables, tk)
		delete(h.store.Partitions, tk)
	}
	delete(h.store.Databases, key)
	h.store.recordEvent("DROP_DATABASE", db, "")
	return nil
}

// GetAllTables lists table names in the given (possibly catalog-qualified)
// database.
func (h *handler) GetAllTables(_ context.Context, dbName string) ([]string, error) {
	h.rec.record("get_all_tables", dbName)
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	prefix := dbKey(catName, db) + "."
	var names []string
	for key, t := range h.store.Tables {
		if strings.HasPrefix(key, prefix) {
			names = append(names, t.TableName)
		}
	}
	slices.Sort(names)
	return names, nil
}

// GetTableReq returns one table.
func (h *handler) GetTableReq(_ context.Context, req *hive_metastore.GetTableRequest) (*hive_metastore.GetTableResult_, error) {
	h.rec.record("get_table_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.Engine != "hive" {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field Engine"}
	}
	if req.ID != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field ID"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	t, ok := h.store.Tables[tblKey(catName, req.DbName, req.TblName)]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	return &hive_metastore.GetTableResult_{Table: t}, nil
}

// GetTableObjectsByNameReq returns tables that exist, in request order,
// silently skipping unknown names.
func (h *handler) GetTableObjectsByNameReq(_ context.Context, req *hive_metastore.GetTablesRequest) (*hive_metastore.GetTablesResult_, error) {
	h.rec.record("get_table_objects_by_name_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	tables := make([]*hive_metastore.Table, 0, len(req.TblNames))
	for _, name := range req.TblNames {
		if t, ok := h.store.Tables[tblKey(catName, req.DbName, name)]; ok {
			tables = append(tables, t)
		}
	}
	return &hive_metastore.GetTablesResult_{Tables: tables}, nil
}

// CreateTable adds a new table.
func (h *handler) CreateTable(_ context.Context, tbl *hive_metastore.Table) error {
	h.rec.record("create_table", tbl)
	catName, err := cat(h.v, tbl.CatName)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, tbl.DbName, tbl.TableName)
	if _, ok := h.store.Tables[key]; ok {
		return &hive_metastore.AlreadyExistsException{Message: "table " + tbl.TableName + " already exists"}
	}
	stored := *tbl
	if stored.CreateTime == 0 {
		stored.CreateTime = now32()
	}
	h.store.Tables[key] = &stored
	h.store.recordEvent("CREATE_TABLE", tbl.DbName, tbl.TableName)
	return nil
}

// AlterTable replaces a table, possibly renaming it.
func (h *handler) AlterTable(_ context.Context, dbName, tblName string, newTbl *hive_metastore.Table) error {
	h.rec.record("alter_table", AlterTableArgs{DbName: dbName, TblName: tblName, NewTbl: newTbl})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	if _, ok := h.store.Tables[key]; !ok {
		return &hive_metastore.MetaException{Message: "table " + db + "." + tblName + " not found"}
	}
	delete(h.store.Tables, key)
	stored := *newTbl
	h.store.Tables[tblKey(catName, db, newTbl.TableName)] = &stored
	h.store.recordEvent("ALTER_TABLE", db, newTbl.TableName)
	return nil
}

// DropTable removes a table and its partitions.
func (h *handler) DropTable(_ context.Context, dbName, name string, deleteData bool) error {
	h.rec.record("drop_table", DropTableArgs{DbName: dbName, TableName: name, DeleteData: deleteData})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, name)
	if _, ok := h.store.Tables[key]; !ok {
		return &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + name + " not found"}
	}
	delete(h.store.Tables, key)
	delete(h.store.Partitions, key)
	h.store.recordEvent("DROP_TABLE", db, name)
	return nil
}

// GetPartitions lists up to maxParts partitions of a table.
func (h *handler) GetPartitions(_ context.Context, dbName, tblName string, maxParts int16) ([]*hive_metastore.Partition, error) {
	h.rec.record("get_partitions", GetPartitionsArgs{DbName: dbName, TblName: tblName, MaxParts: maxParts})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	if _, ok := h.store.Tables[key]; !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + tblName + " not found"}
	}
	return truncateParts(h.store.Partitions[key], maxParts), nil
}

// GetPartitionsReq lists up to req.MaxParts partitions of a table.
func (h *handler) GetPartitionsReq(_ context.Context, req *hive_metastore.PartitionsRequest) (*hive_metastore.PartitionsResponse, error) {
	h.rec.record("get_partitions_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.ID != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field ID"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, req.DbName, req.TblName)
	if _, ok := h.store.Tables[key]; !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	return &hive_metastore.PartitionsResponse{Partitions: truncateParts(h.store.Partitions[key], req.MaxParts)}, nil
}

// GetPartitionNames lists up to maxParts partition names, formatted as
// "k1=v1/k2=v2" from the table's PartitionKeys and each partition's Values.
func (h *handler) GetPartitionNames(_ context.Context, dbName, tblName string, maxParts int16) ([]string, error) {
	h.rec.record("get_partition_names", GetPartitionNamesArgs{DbName: dbName, TblName: tblName, MaxParts: maxParts})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + tblName + " not found"}
	}
	parts := truncateParts(h.store.Partitions[key], maxParts)
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = partitionName(tbl, p)
	}
	return names, nil
}

// AddPartitionsReq adds partitions to a table. With IfNotExists set,
// partitions whose values already exist are silently skipped; otherwise
// they are reported as AlreadyExistsException.
func (h *handler) AddPartitionsReq(_ context.Context, req *hive_metastore.AddPartitionsRequest) (*hive_metastore.AddPartitionsResult_, error) {
	h.rec.record("add_partitions_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, req.DbName, req.TblName)
	if _, ok := h.store.Tables[key]; !ok {
		return nil, &hive_metastore.MetaException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	existing := h.store.Partitions[key]
	added := make([]*hive_metastore.Partition, 0, len(req.Parts))
	for _, p := range req.Parts {
		if findPartition(existing, p.Values) >= 0 {
			if !req.IfNotExists {
				return nil, &hive_metastore.AlreadyExistsException{Message: "partition already exists"}
			}
			continue
		}
		stored := *p
		if stored.CreateTime == 0 {
			stored.CreateTime = now32()
		}
		existing = append(existing, &stored)
		added = append(added, &stored)
	}
	h.store.Partitions[key] = existing
	if len(added) > 0 {
		h.store.recordEvent("ADD_PARTITION", req.DbName, req.TblName)
	}
	return &hive_metastore.AddPartitionsResult_{Partitions: added}, nil
}

// applyAlterPartitions replaces existing partitions matching by Values.
// The store lock must be held by the caller.
func (h *handler) applyAlterPartitions(catName, db, tbl string, parts []*hive_metastore.Partition) error {
	key := tblKey(catName, db, tbl)
	if _, ok := h.store.Tables[key]; !ok {
		return &hive_metastore.MetaException{Message: "table " + db + "." + tbl + " not found"}
	}
	existing := h.store.Partitions[key]

	// Resolve every target index before mutating anything, so a batch
	// that fails partway through (one Values tuple not found) leaves the
	// store's existing partitions completely untouched rather than
	// partially altered.
	idxs := make([]int, len(parts))
	for i, np := range parts {
		idx := findPartition(existing, np.Values)
		if idx < 0 {
			return &hive_metastore.InvalidOperationException{Message: "partition not found"}
		}
		idxs[i] = idx
	}
	for i, np := range parts {
		existing[idxs[i]] = np
	}
	h.store.Partitions[key] = existing
	return nil
}

// AlterPartitions replaces existing partitions of a table.
func (h *handler) AlterPartitions(_ context.Context, dbName, tblName string, parts []*hive_metastore.Partition) error {
	h.rec.record("alter_partitions", AlterPartitionsArgs{DbName: dbName, TblName: tblName, Parts: parts})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	return h.applyAlterPartitions(catName, db, tblName, parts)
}

// AlterPartitionsReq replaces existing partitions of a table.
func (h *handler) AlterPartitionsReq(_ context.Context, req *hive_metastore.AlterPartitionsRequest) (*hive_metastore.AlterPartitionsResponse, error) {
	h.rec.record("alter_partitions_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.WriteId != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field WriteId"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if err := h.applyAlterPartitions(catName, req.DbName, req.TableName, req.Partitions); err != nil {
		return nil, err
	}
	return &hive_metastore.AlterPartitionsResponse{}, nil
}

// DropPartition removes one partition identified by its values.
func (h *handler) DropPartition(_ context.Context, dbName, tblName string, partVals []string, deleteData bool) (bool, error) {
	h.rec.record("drop_partition", DropPartitionArgs{DbName: dbName, TblName: tblName, PartVals: partVals, DeleteData: deleteData})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return false, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	existing := h.store.Partitions[key]
	idx := findPartition(existing, partVals)
	if idx < 0 {
		return false, &hive_metastore.NoSuchObjectException{Message: "partition not found"}
	}
	h.store.Partitions[key] = append(existing[:idx], existing[idx+1:]...)
	h.store.recordEvent("DROP_PARTITION", db, tblName)
	return true, nil
}

// DropPartitionsReq removes the partitions of a table named by
// req.Parts.Names -- the only RequestPartsSpec arm hms-client-go ever
// sends (see newDropPartitionsRequest in partition.go); Exprs is rejected
// below rather than implemented, since nothing in this module's tests
// populates it. Every other field with no exposed equivalent on this
// package's DropPartitionsByNames/DropPartitions (IgnoreProtection,
// EnvironmentContext, SkipColumnSchemaForPartition, and NeedResult_ which
// this package always turns off) is likewise rejected if the caller sent
// anything but the default, mirroring GetPartitionsReq's treatment of ID
// and AlterPartitionsReq's treatment of WriteId. With req.IfExists true, a
// name matching no existing partition is silently skipped, mirroring
// DropPartition's own ifExists handling; with it false, the first such
// name is reported as NoSuchObjectException and nothing named after it in
// Parts.Names is dropped, mirroring drop_partitions_req's own "stop at the
// first missing name" behavior on a real server.
func (h *handler) DropPartitionsReq(_ context.Context, req *hive_metastore.DropPartitionsRequest) (*hive_metastore.DropPartitionsResult_, error) {
	h.rec.record("drop_partitions_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.IgnoreProtection != nil {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field IgnoreProtection"}
	}
	if req.EnvironmentContext != nil {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field EnvironmentContext"}
	}
	if req.SkipColumnSchemaForPartition != nil {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field SkipColumnSchemaForPartition"}
	}
	if req.NeedResult_ {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field NeedResult_"}
	}
	if req.Parts == nil || len(req.Parts.Exprs) != 0 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field Parts.Exprs"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, req.DbName, req.TblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	existing := h.store.Partitions[key]
	for _, name := range req.Parts.Names {
		idx := -1
		for i, p := range existing {
			if partitionName(tbl, p) == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			if req.IfExists {
				continue
			}
			return nil, &hive_metastore.NoSuchObjectException{Message: "partition " + name + " not found"}
		}
		existing = append(existing[:idx], existing[idx+1:]...)
	}
	h.store.Partitions[key] = existing
	h.store.recordEvent("DROP_PARTITION", req.DbName, req.TblName)
	return &hive_metastore.DropPartitionsResult_{}, nil
}

// GetPartitionsByNames returns the partitions of a table whose computed
// name (partitionName) is in names.
func (h *handler) GetPartitionsByNames(_ context.Context, dbName, tblName string, names []string) ([]*hive_metastore.Partition, error) {
	h.rec.record("get_partitions_by_names", GetPartitionsByNamesArgs{DbName: dbName, TblName: tblName, Names: names})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + tblName + " not found"}
	}
	return partitionsMatchingNames(tbl, h.store.Partitions[key], names), nil
}

// GetPartitionsByNamesReq is GetPartitionsByNames' request-variant RPC. Its
// request struct carries no CatName field (unlike GetPartitionNamesPsReq):
// a non-default catalog is instead expressed as a "@<cat>#<db>" qualifier
// on DbName, the same convention get_partitions_by_names itself uses (see
// newGetPartitionsByNamesRequest in partition.go).
func (h *handler) GetPartitionsByNamesReq(_ context.Context, req *hive_metastore.GetPartitionsByNamesRequest) (*hive_metastore.GetPartitionsByNamesResult_, error) {
	h.rec.record("get_partitions_by_names_req", req)
	if req.Engine != "hive" {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field Engine"}
	}
	if req.ID != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field ID"}
	}
	catName, db, err := resolveDB(h.v, req.DbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, req.TblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + req.TblName + " not found"}
	}
	return &hive_metastore.GetPartitionsByNamesResult_{Partitions: partitionsMatchingNames(tbl, h.store.Partitions[key], req.Names)}, nil
}

// GetPartitionsByFilter returns up to maxParts partitions of a table
// matching filter (see parsePartitionFilter for the supported subset).
func (h *handler) GetPartitionsByFilter(_ context.Context, dbName, tblName, filter string, maxParts int16) ([]*hive_metastore.Partition, error) {
	h.rec.record("get_partitions_by_filter", GetPartitionsByFilterArgs{DbName: dbName, TblName: tblName, Filter: filter, MaxParts: maxParts})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	terms, err := parsePartitionFilter(filter)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + tblName + " not found"}
	}
	var matched []*hive_metastore.Partition
	for _, p := range h.store.Partitions[key] {
		if partitionMatchesFilterTerms(tbl, p, terms) {
			matched = append(matched, p)
		}
	}
	return truncateParts(matched, maxParts), nil
}

// GetPartitionNamesPs returns up to maxParts partition names of a table
// whose Values match partVals as a prefix (see
// partitionMatchesPartialValues).
func (h *handler) GetPartitionNamesPs(_ context.Context, dbName, tblName string, partVals []string, maxParts int16) ([]string, error) {
	h.rec.record("get_partition_names_ps", GetPartitionNamesPsArgs{DbName: dbName, TblName: tblName, PartVals: partVals, MaxParts: maxParts})
	catName, db, err := resolveDB(h.v, dbName)
	if err != nil {
		return nil, err
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, db, tblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + db + "." + tblName + " not found"}
	}
	matched := truncateParts(partitionsMatchingPartialValues(h.store.Partitions[key], partVals), maxParts)
	names := make([]string, len(matched))
	for i, p := range matched {
		names[i] = partitionName(tbl, p)
	}
	return names, nil
}

// GetPartitionNamesPsReq is GetPartitionNamesPs' request-variant RPC.
// Unlike GetPartitionsByNamesReq, its request struct does carry a CatName
// field, resolved the same way get_partitions_req's does (see the cat
// helper).
func (h *handler) GetPartitionNamesPsReq(_ context.Context, req *hive_metastore.GetPartitionNamesPsRequest) (*hive_metastore.GetPartitionNamesPsResponse, error) {
	h.rec.record("get_partition_names_ps_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.ID != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field ID"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, req.DbName, req.TblName)
	tbl, ok := h.store.Tables[key]
	if !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	matched := truncateParts(partitionsMatchingPartialValues(h.store.Partitions[key], req.PartValues), req.MaxParts)
	names := make([]string, len(matched))
	for i, p := range matched {
		names[i] = partitionName(tbl, p)
	}
	return &hive_metastore.GetPartitionNamesPsResponse{Names: names}, nil
}

// GetCurrentNotificationEventId returns the ID of the most recently
// recorded notification event, or 0 when the event log is empty.
func (h *handler) GetCurrentNotificationEventId(_ context.Context) (*hive_metastore.CurrentNotificationEventId, error) {
	h.rec.record("get_current_notificationEventId", nil)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	var id int64
	if n := len(h.store.Events); n > 0 {
		id = h.store.Events[n-1].EventId
	}
	return &hive_metastore.CurrentNotificationEventId{EventId: id}, nil
}

// GetNextNotification returns the events recorded after req.LastEvent,
// oldest first, honouring req.MaxEvents (no limit when unset or <= 0).
// req.EventTypeList is honoured server-side only on Hive40, mirroring a
// real 2.3/3.x metastore, whose NotificationEventRequest predates that
// field entirely (SPEC §2.1; see notification.go's doc comment on
// newNotificationEventRequest for why the client additionally filters the
// response locally).
func (h *handler) GetNextNotification(_ context.Context, req *hive_metastore.NotificationEventRequest) (*hive_metastore.NotificationEventResponse, error) {
	h.rec.record("get_next_notification", req)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	var want map[string]bool
	if h.v == Hive40 && len(req.EventTypeList) > 0 {
		want = make(map[string]bool, len(req.EventTypeList))
		for _, t := range req.EventTypeList {
			want[t] = true
		}
	}

	var out []*hive_metastore.NotificationEvent
	for _, e := range h.store.Events {
		if e.EventId <= req.LastEvent {
			continue
		}
		if want != nil && !want[e.EventType] {
			continue
		}
		out = append(out, e)
		if req.MaxEvents != nil && *req.MaxEvents > 0 && len(out) >= int(*req.MaxEvents) {
			break
		}
	}
	return &hive_metastore.NotificationEventResponse{Events: out}, nil
}

// GetTableStatisticsReq returns the column statistics seeded (via
// Server.SeedColumnStats) for the table named req.TblName in database
// req.DbName, filtered to the columns named in req.ColNames: a column with
// no seeded entry -- including one req.ColNames names but SeedColumnStats
// never seeded -- is simply absent from the result, matching a real
// server's per-column stat availability rather than an error. It rejects a
// non-default Engine or ID exactly as GetTableReq/GetPartitionsReq do
// (SPEC §2.3, §5.8), since TableStatsRequest carries the same 4.x-only
// Engine/ID fields those requests do.
func (h *handler) GetTableStatisticsReq(_ context.Context, req *hive_metastore.TableStatsRequest) (*hive_metastore.TableStatsResult_, error) {
	h.rec.record("get_table_statistics_req", req)
	catName, err := cat(h.v, req.CatName)
	if err != nil {
		return nil, err
	}
	if req.Engine != "hive" {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field Engine"}
	}
	if req.ID != -1 {
		return nil, &hive_metastore.MetaException{Message: "unexpected non-default field ID"}
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	key := tblKey(catName, req.DbName, req.TblName)
	if _, ok := h.store.Tables[key]; !ok {
		return nil, &hive_metastore.NoSuchObjectException{Message: "table " + req.DbName + "." + req.TblName + " not found"}
	}
	want := make(map[string]bool, len(req.ColNames))
	for _, n := range req.ColNames {
		want[n] = true
	}
	var out []*hive_metastore.ColumnStatisticsObj
	for _, o := range h.store.ColumnStats[key] {
		if want[o.ColName] {
			out = append(out, o)
		}
	}
	return &hive_metastore.TableStatsResult_{TableStats: out}, nil
}
