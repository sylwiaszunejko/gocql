// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"unicode"
)

// sortedByName returns a metadata map's values ordered by key. ToCQL walks
// five such maps, and Go randomises map iteration, so without this a
// regenerated dump reorders its own sections between runs -- three tables alone
// produce three different outputs. Types are ordered separately, by dependency
// rather than by name.
func sortedByName[T any](m map[string]T) []T {
	out := make([]T, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, m[k])
	}
	return out
}

// ToCQL returns a CQL query that ca be used to recreate keyspace with all
// user defined types, tables, indexes, functions, aggregates and views associated
// with this keyspace.
func (ks *KeyspaceMetadata) ToCQL() (string, error) {
	// Be aware that `CreateStmts` is not only a cache for ToCQL,
	// but it also can be populated from response to `DESCRIBE KEYSPACE %s WITH INTERNALS`
	if len(ks.CreateStmts) != 0 {
		return ks.CreateStmts, nil
	}

	var sb strings.Builder

	if err := ks.keyspaceToCQL(&sb); err != nil {
		return "", err
	}

	sortedTypes := ks.typesSortedTopologically()
	for _, tm := range sortedTypes {
		if err := ks.userTypeToCQL(&sb, tm); err != nil {
			return "", err
		}
	}

	for _, tm := range sortedByName(ks.Tables) {
		if err := ks.tableToCQL(&sb, ks.Name, tm); err != nil {
			return "", err
		}
	}

	for _, im := range sortedByName(ks.Indexes) {
		if err := ks.indexToCQL(&sb, im); err != nil {
			return "", err
		}
	}

	for _, fm := range sortedByName(ks.Functions) {
		if err := ks.functionToCQL(&sb, ks.Name, fm); err != nil {
			return "", err
		}
	}

	for _, am := range sortedByName(ks.Aggregates) {
		if err := ks.aggregateToCQL(&sb, am); err != nil {
			return "", err
		}
	}

	for _, vm := range sortedByName(ks.Views) {
		if err := ks.viewToCQL(&sb, vm); err != nil {
			return "", err
		}
	}

	ks.CreateStmts = sb.String()
	return ks.CreateStmts, nil
}

// cqlTypeRef is one type name appearing in a rendered CQL type, with the three
// facts needed to tell a user type from a built-in.
type cqlTypeRef struct {
	name string
	// quoted: the name was written in double quotes, so it is a user type --
	// built-ins are never quoted in what the server stores.
	quoted bool
	// parameterised: the name is immediately followed by '<', so it is being
	// applied to type arguments. That is what separates the collection in
	// frozen<map<text,int>> from a reference to a user type named map, which
	// the server stores unquoted as frozen<map>.
	parameterised bool
	// frozenOperand: the name is the direct operand of frozen<...>. frozen
	// applies to collections, tuples and user types, never to a scalar
	// built-in, and a user type used as a field of another must be frozen. So
	// this is what separates the built-in text in a field declared "text" from
	// the user type text in one declared frozen<text> -- which is exactly how
	// system_schema.types stores the two.
	frozenOperand bool
}

// cqlTypeIdentifiers splits a rendered CQL type into the names it mentions:
// frozen<addr> yields frozen and addr, varchar yields varchar rather than
// appearing to contain a type named "var".
//
// Quoted identifiers are taken atomically, because system_schema.types stores
// the reference with its quotes while keying the type by the unquoted name: a
// UDT "z-type" is keyed z-type and referenced as frozen<"z-type">. Splitting on
// punctuation would yield "z" and "type" and lose the dependency entirely.
// A doubled quote inside the quotes is one literal quote, as in CQL.
func cqlTypeIdentifiers(cqlType string) []cqlTypeRef {
	var out []cqlTypeRef
	rs := []rune(cqlType)
	pendingFrozen := false

	// parameterisedAt reports whether the next non-space rune from i is '<'.
	parameterisedAt := func(i int) bool {
		for i < len(rs) && unicode.IsSpace(rs[i]) {
			i++
		}
		return i < len(rs) && rs[i] == '<'
	}

	emit := func(name string, quoted bool, parameterised bool) {
		// The operand slot opened by a preceding frozen< is consumed by
		// whatever comes next, including a token that is dropped below.
		// Clearing it here stops the token after a dropped one inheriting a
		// frozen< that has already been used up.
		frozenOperand := pendingFrozen
		pendingFrozen = false

		// An unquoted run starting with a digit is a literal, not an
		// identifier: the N in vector<float, 3> is a dimension, not a type.
		if name == "" || (!quoted && !isCQLIdentifierStart([]rune(name)[0])) {
			return
		}
		out = append(out, cqlTypeRef{
			name: name, quoted: quoted,
			parameterised: parameterised, frozenOperand: frozenOperand,
		})
		// Only an applied frozen opens an operand slot. A bare frozen is not
		// valid CQL, but arming on it would hand the slot to whatever token
		// came next -- the same leak the dropped-token path above avoids.
		pendingFrozen = !quoted && parameterised && name == "frozen"
	}

	for i := 0; i < len(rs); {
		switch {
		case rs[i] == '"':
			var sb strings.Builder
			i++
			for i < len(rs) {
				if rs[i] == '"' {
					if i+1 < len(rs) && rs[i+1] == '"' {
						sb.WriteRune('"')
						i += 2
						continue
					}
					i++
					break
				}
				sb.WriteRune(rs[i])
				i++
			}
			emit(sb.String(), true, parameterisedAt(i))

		case rs[i] == '\'':
			// A single-quoted run is a custom marshal type such as
			// 'org.apache.cassandra.db.marshal.UTF8Type', which CQL allows
			// wherever a field type is written. Its contents are a Java class
			// name, never a user type, so the whole literal is consumed rather
			// than tokenised -- otherwise org, apache and so on are offered as
			// dependencies. emit with an empty name discards it while still
			// consuming the operand slot a preceding frozen< opened. A doubled
			// quote inside is one literal quote, as in CQL.
			i++
			for i < len(rs) {
				if rs[i] == '\'' {
					if i+1 < len(rs) && rs[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			emit("", false, false)

		case isCQLIdentifierRune(rs[i]):
			j := i
			for j < len(rs) && isCQLIdentifierRune(rs[j]) {
				j++
			}
			emit(string(rs[i:j]), false, parameterisedAt(j))
			i = j

		default:
			i++
		}
	}

	return out
}

func isCQLIdentifierRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isCQLIdentifierStart reports whether r may begin an unquoted identifier. A
// digit may appear inside one but never at the front.
func isCQLIdentifierStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

// cqlTypeConstructors are the parameterised type spellings. Applied to type
// arguments they are never a user type, not even as the operand of frozen:
// frozen<list<text>> is a frozen collection, not a reference to a type named
// list. Written without arguments the name is available to a user type, which
// is why this is only decisive together with parameterised.
var cqlTypeConstructors = map[string]struct{}{
	"frozen": {}, "list": {}, "map": {}, "set": {}, "tuple": {}, "vector": {},
}

// cqlNativeTypes are the scalar built-ins. A keyspace may still define a user
// type with one of these names -- it has to be quoted at creation -- which is
// why a bare occurrence is only assumed to be the built-in when it is not the
// operand of frozen.
var cqlNativeTypes = map[string]struct{}{
	"ascii": {}, "bigint": {}, "blob": {}, "boolean": {}, "counter": {},
	"date": {}, "decimal": {}, "double": {}, "duration": {}, "float": {},
	"inet": {}, "int": {}, "smallint": {}, "text": {}, "time": {},
	"timestamp": {}, "timeuuid": {}, "tinyint": {}, "uuid": {}, "varchar": {},
	"varint": {},
}

// namesUserType reports whether ref can denote a user-defined type, as opposed
// to a built-in that happens to share its name.
func (ref cqlTypeRef) namesUserType() bool {
	// A quoted name is always a user type; the server only keeps quotes where
	// the name needs them, and never quotes a built-in.
	if ref.quoted {
		return true
	}
	// Checked before frozenOperand: the operand of frozen is a user type
	// unless it is itself a constructor being applied to arguments.
	if _, ctor := cqlTypeConstructors[ref.name]; ctor && ref.parameterised {
		return false
	}
	if ref.frozenOperand {
		return true
	}
	_, native := cqlNativeTypes[ref.name]
	return !native
}

// typesSortedTopologically orders the keyspace's user-defined types so that
// every type appears after the types it embeds, which is the order a server
// needs to replay the CREATE TYPE statements.
//
// This has to be a graph traversal rather than a comparison sort. "j embeds i"
// is not a transitive relation, so for a chain a <- b <- c a comparison sort may
// compare only (b,a) and (c,b), never (c,a), and leave a type ahead of one it
// embeds depending on the order it happened to start from.
//
// Dependencies are matched on whole CQL identifiers. A field type is rendered
// CQL such as frozen<addr> or map<text, frozen<addr>>, so the names it depends
// on are its identifiers minus the built-ins. Matching substrings instead is
// not merely over-constraining: a type "a" with a varchar field yields a false
// a -> var edge, which closes a cycle with the real var -> a edge if a type
// "var" embeds "a", and the cycle guard then resolves it the wrong way round --
// emitting var before the a it embeds.
func (ks *KeyspaceMetadata) typesSortedTopologically() []*TypeMetadata {
	sorted := make([]*TypeMetadata, 0, len(ks.Types))
	visited := make(map[string]bool, len(ks.Types))

	var visit func(name string)
	visit = func(name string) {
		tm, ok := ks.Types[name]
		if !ok || visited[name] {
			return
		}
		// Marked before recursing, so a cycle terminates instead of recursing
		// forever. CQL rejects cyclic user types, so this is a guard rather
		// than a case with a defined ordering.
		visited[name] = true

		for _, ft := range tm.FieldTypes {
			for _, ref := range cqlTypeIdentifiers(ft) {
				// A bare built-in is skipped rather than followed. Following it
				// would be harmless were there no user type of that name, but a
				// keyspace may define one: the false edge then closes a cycle
				// with the real one and the walk emits the dependent first.
				if ref.name != name && ref.namesUserType() {
					visit(ref.name)
				}
			}
		}
		sorted = append(sorted, tm)
	}

	// Visit in name order so the output does not depend on map iteration order.
	for _, name := range slices.Sorted(maps.Keys(ks.Types)) {
		visit(name)
	}
	return sorted
}

var tableCQLTemplate = template.Must(template.New("table").
	Funcs(map[string]any{
		"escape":               cqlHelpers.escape,
		"tableColumnToCQL":     cqlHelpers.tableColumnToCQL,
		"tablePropertiesToCQL": cqlHelpers.tablePropertiesToCQL,
	}).
	Parse(`
CREATE TABLE {{ .KeyspaceName }}.{{ .Tm.Name }} (
    {{ tableColumnToCQL .Tm }}
) WITH {{ tablePropertiesToCQL .Tm.ClusteringColumns .Tm.Options .Tm.Extensions }};
`))

func (ks *KeyspaceMetadata) tableToCQL(w io.Writer, kn string, tm *TableMetadata) error {
	if err := tableCQLTemplate.Execute(w, map[string]any{
		"Tm":           tm,
		"KeyspaceName": kn,
	}); err != nil {
		return err
	}
	return nil
}

var functionTemplate = template.Must(template.New("functions").
	Funcs(map[string]any{
		"escape":      cqlHelpers.escape,
		"zip":         cqlHelpers.zip,
		"stripFrozen": cqlHelpers.stripFrozen,
	}).
	Parse(`
CREATE FUNCTION {{ .keyspaceName }}.{{ .fm.Name }} ( 
    {{- range $i, $args := zip .fm.ArgumentNames .fm.ArgumentTypes }}
    {{- if ne $i 0 }}, {{ end }}
    {{- (index $args 0) }}
    {{ stripFrozen (index $args 1) }}
    {{- end -}})
    {{ if .fm.CalledOnNullInput }}CALLED{{ else }}RETURNS NULL{{ end }} ON NULL INPUT
    RETURNS {{ .fm.ReturnType }}
    LANGUAGE {{ .fm.Language }}
    AS $${{ .fm.Body }}$$;
`))

func (ks *KeyspaceMetadata) functionToCQL(w io.Writer, keyspaceName string, fm *FunctionMetadata) error {
	if err := functionTemplate.Execute(w, map[string]any{
		"fm":           fm,
		"keyspaceName": keyspaceName,
	}); err != nil {
		return err
	}
	return nil
}

var viewTemplate = template.Must(template.New("views").
	Funcs(map[string]any{
		"zip":                  cqlHelpers.zip,
		"partitionKeyString":   cqlHelpers.partitionKeyString,
		"tablePropertiesToCQL": cqlHelpers.tablePropertiesToCQL,
	}).
	Parse(`
CREATE MATERIALIZED VIEW {{ .vm.KeyspaceName }}.{{ .vm.ViewName }} AS
    SELECT {{ if .vm.IncludeAllColumns }}*{{ else }}
    {{- range $i, $col := .vm.OrderedColumns }}
    {{- if ne $i 0 }}, {{ end }}
    {{ $col }}
    {{- end }}
    {{- end }}
    FROM {{ .vm.KeyspaceName }}.{{ .vm.BaseTableName }}
    WHERE {{ .vm.WhereClause }}
    PRIMARY KEY ({{ partitionKeyString .vm.PartitionKey .vm.ClusteringColumns }})
    WITH {{ tablePropertiesToCQL .vm.ClusteringColumns .vm.Options .vm.Extensions }};
`))

func (ks *KeyspaceMetadata) viewToCQL(w io.Writer, vm *ViewMetadata) error {
	if err := viewTemplate.Execute(w, map[string]any{
		"vm": vm,
	}); err != nil {
		return err
	}
	return nil
}

var aggregatesTemplate = template.Must(template.New("aggregate").
	Funcs(map[string]any{
		"stripFrozen": cqlHelpers.stripFrozen,
	}).
	Parse(`
CREATE AGGREGATE {{ .Keyspace }}.{{ .Name }}( 
    {{- range $i, $arg := .ArgumentTypes }}
    {{- if ne $i 0 }}, {{ end }}
    {{ stripFrozen $arg }}
    {{- end -}})
    SFUNC {{ .StateFunc.Name }}
    STYPE {{ stripFrozen .StateType }}
    {{- if ne .FinalFunc.Name "" }}
    FINALFUNC {{ .FinalFunc.Name }}
    {{- end -}}
    {{- if ne .InitCond "" }}
    INITCOND {{ .InitCond }}
    {{- end -}}
;
`))

func (ks *KeyspaceMetadata) aggregateToCQL(w io.Writer, am *AggregateMetadata) error {
	if err := aggregatesTemplate.Execute(w, am); err != nil {
		return err
	}
	return nil
}

var typeCQLTemplate = template.Must(template.New("types").
	Funcs(map[string]any{
		"zip": cqlHelpers.zip,
	}).
	Parse(`
CREATE TYPE {{ .Keyspace }}.{{ .Name }} ( 
  {{- range $i, $fields := zip .FieldNames .FieldTypes }} {{- if ne $i 0 }},{{ end }}
    {{ index $fields 0 }} {{ index $fields 1 }}
  {{- end }}
);
`))

func (ks *KeyspaceMetadata) userTypeToCQL(w io.Writer, tm *TypeMetadata) error {
	if err := typeCQLTemplate.Execute(w, tm); err != nil {
		return err
	}
	return nil
}

func (ks *KeyspaceMetadata) indexToCQL(w io.Writer, im *IndexMetadata) error {
	// Scylla doesn't support any custom indexes
	if im.Kind == IndexKindCustom {
		return nil
	}

	options := im.Options
	indexTarget := options["target"]

	// secondary index
	si := struct {
		ClusteringKeys []string `json:"ck"`
		PartitionKeys  []string `json:"pk"`
	}{}

	if err := json.Unmarshal([]byte(indexTarget), &si); err == nil {
		indexTarget = fmt.Sprintf("(%s), %s",
			strings.Join(si.PartitionKeys, ","),
			strings.Join(si.ClusteringKeys, ","),
		)
	}

	_, err := fmt.Fprintf(w, "\nCREATE INDEX %s ON %s.%s (%s);\n",
		im.Name,
		im.KeyspaceName,
		im.TableName,
		indexTarget,
	)
	if err != nil {
		return err
	}

	return nil
}

var keyspaceCQLTemplate = template.Must(template.New("keyspace").
	Funcs(map[string]any{
		"escape":      cqlHelpers.escape,
		"fixStrategy": cqlHelpers.fixStrategy,
	}).
	// Single-line, always-explicit durable_writes to match what DESCRIBE KEYSPACE
	// returns from Cassandra/Scylla, so this fallback stays consistent with the
	// server-echoed CreateStmts path in ToCQL.
	Parse(`CREATE KEYSPACE {{ .Name }} WITH replication = {'class': {{ escape ( fixStrategy .StrategyClass) }}{{ range $key, $value := .StrategyOptions }}, {{ escape $key }}: {{ escape $value }}{{ end }}} AND durable_writes = {{ .DurableWrites }};
`))

func (ks *KeyspaceMetadata) keyspaceToCQL(w io.Writer) error {
	if err := keyspaceCQLTemplate.Execute(w, ks); err != nil {
		return err
	}
	return nil
}

type toCQLHelpers struct{}

var cqlHelpers = toCQLHelpers{}

func (h toCQLHelpers) zip(a []string, b []string) [][]string {
	m := make([][]string, len(a))
	for i := range a {
		m[i] = []string{a[i], b[i]}
	}
	return m
}

func (h toCQLHelpers) escape(e any) string {
	switch v := e.(type) {
	case int, float64:
		return fmt.Sprint(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return string(v)
	}
	return ""
}

func (h toCQLHelpers) stripFrozen(v string) string {
	return strings.TrimSuffix(strings.TrimPrefix(v, "frozen<"), ">")
}
func (h toCQLHelpers) fixStrategy(v string) string {
	return strings.TrimPrefix(v, "org.apache.cassandra.locator.")
}

func (h toCQLHelpers) fixQuote(v string) string {
	return strings.ReplaceAll(v, `"`, `'`)
}

func (h toCQLHelpers) tableOptionsToCQL(ops TableMetadataOptions) ([]string, error) {
	opts := map[string]any{
		"bloom_filter_fp_chance":      ops.BloomFilterFpChance,
		"comment":                     ops.Comment,
		"crc_check_chance":            ops.CrcCheckChance,
		"default_time_to_live":        ops.DefaultTimeToLive,
		"gc_grace_seconds":            ops.GcGraceSeconds,
		"max_index_interval":          ops.MaxIndexInterval,
		"memtable_flush_period_in_ms": ops.MemtableFlushPeriodInMs,
		"min_index_interval":          ops.MinIndexInterval,
		"speculative_retry":           ops.SpeculativeRetry,
	}

	var err error
	opts["caching"], err = json.Marshal(ops.Caching)
	if err != nil {
		return nil, err
	}

	opts["compaction"], err = json.Marshal(ops.Compaction)
	if err != nil {
		return nil, err
	}

	opts["compression"], err = json.Marshal(ops.Compression)
	if err != nil {
		return nil, err
	}

	cdc, err := json.Marshal(ops.CDC)
	if err != nil {
		return nil, err
	}

	if string(cdc) != "null" {
		opts["cdc"] = cdc
	}

	if ops.InMemory {
		opts["in_memory"] = ops.InMemory
	}

	out := make([]string, 0, len(opts))
	for key, opt := range opts {
		out = append(out, fmt.Sprintf("%s = %s", key, h.fixQuote(h.escape(opt))))
	}

	sort.Strings(out)
	return out, nil
}

func (h toCQLHelpers) tableExtensionsToCQL(extensions map[string]any) ([]string, error) {
	exts := map[string]any{}

	if blob, ok := extensions["scylla_encryption_options"]; ok {
		encOpts := &scyllaEncryptionOptions{}
		if err := encOpts.UnmarshalBinary(blob.([]byte)); err != nil {
			return nil, err
		}

		var err error
		exts["scylla_encryption_options"], err = json.Marshal(encOpts)
		if err != nil {
			return nil, err
		}

	}

	out := make([]string, 0, len(exts))
	for key, ext := range exts {
		out = append(out, fmt.Sprintf("%s = %s", key, h.fixQuote(h.escape(ext))))
	}

	sort.Strings(out)
	return out, nil
}

func (h toCQLHelpers) tablePropertiesToCQL(cks []*ColumnMetadata, opts TableMetadataOptions,
	extensions map[string]any) (string, error) {
	var sb strings.Builder

	var properties []string

	if len(cks) > 0 {
		var inner []string
		for _, col := range cks {
			inner = append(inner, fmt.Sprintf("%s %s", col.Name, col.ClusteringOrder))
		}
		properties = append(properties, fmt.Sprintf("CLUSTERING ORDER BY (%s)", strings.Join(inner, ", ")))
	}

	options, err := h.tableOptionsToCQL(opts)
	if err != nil {
		return "", err
	}
	properties = append(properties, options...)

	exts, err := h.tableExtensionsToCQL(extensions)
	if err != nil {
		return "", err
	}
	properties = append(properties, exts...)

	sb.WriteString(strings.Join(properties, "\n    AND "))
	return sb.String(), nil
}

func (h toCQLHelpers) tableColumnToCQL(tm *TableMetadata) string {
	var sb strings.Builder

	var columns []string
	for _, cn := range tm.OrderedColumns {
		cm := tm.Columns[cn]
		column := fmt.Sprintf("%s %s", cn, cm.Type)
		if cm.Kind == ColumnStatic {
			column += " static"
		}
		columns = append(columns, column)
	}
	if len(tm.PartitionKey) == 1 && len(tm.ClusteringColumns) == 0 && len(columns) > 0 {
		columns[0] += " PRIMARY KEY"
	}

	sb.WriteString(strings.Join(columns, ",\n    "))

	if len(tm.PartitionKey) > 1 || len(tm.ClusteringColumns) > 0 {
		sb.WriteString(",\n    PRIMARY KEY (")
		sb.WriteString(h.partitionKeyString(tm.PartitionKey, tm.ClusteringColumns))
		sb.WriteRune(')')
	}

	return sb.String()
}

func (h toCQLHelpers) partitionKeyString(pks, cks []*ColumnMetadata) string {
	var sb strings.Builder

	if len(pks) > 1 {
		sb.WriteRune('(')
		for i, pk := range pks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pk.Name)
		}
		sb.WriteRune(')')
	} else {
		sb.WriteString(pks[0].Name)
	}

	if len(cks) > 0 {
		sb.WriteString(", ")
		for i, ck := range cks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ck.Name)
		}
	}

	return sb.String()
}

type scyllaEncryptionOptions struct {
	CipherAlgorithm   string `json:"cipher_algorithm"`
	KeyProvider       string `json:"key_provider"`
	SecretKeyFile     string `json:"secret_key_file"`
	SecretKeyStrength int    `json:"secret_key_strength"`
}

// UnmarshalBinary deserializes blob into scyllaEncryptionOptions.
// Format:
//   - 4 bytes - size of KV map
//     Size times:
//   - 4 bytes - length of key
//   - len_of_key bytes - key
//   - 4 bytes - length of value
//   - len_of_value bytes - value
func (enc *scyllaEncryptionOptions) UnmarshalBinary(data []byte) error {
	size := binary.LittleEndian.Uint32(data[0:4])

	m := make(map[string]string, size)

	off := uint32(4)
	for i := uint32(0); i < size; i++ {
		keyLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4

		key := string(data[off : off+keyLen])
		off += keyLen

		valueLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4

		value := string(data[off : off+valueLen])
		off += valueLen

		m[key] = value
	}

	enc.CipherAlgorithm = m["cipher_algorithm"]
	enc.KeyProvider = m["key_provider"]
	enc.SecretKeyFile = m["secret_key_file"]
	if secretKeyStrength, ok := m["secret_key_strength"]; ok {
		sks, err := strconv.Atoi(secretKeyStrength)
		if err != nil {
			return err
		}
		enc.SecretKeyStrength = sks
	}

	return nil
}
