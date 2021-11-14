package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	SQL    *sql.DB
	stmt   *sql.Stmt
	rows   *sql.Rows
	config *Config
	params sync.Pool
	pk     string
	table  string
	alias  string
	field  string
	force  string
	where  string
	order  string
	limit  string
	LastId int64
	RowNum int64
}

func Open(cfg *Config) *DB {
	cfg = cfg.Configure()

	opt := []string{
		cfg.Username,
		":",
		cfg.Password,
		"@tcp(",
		cfg.Host,
		":",
		cfg.Port,
		")/",
	}

	if cfg.useDb {
		opt = []string{
			strings.Join(opt, ""),
			cfg.Database,
			"?charset=",
			cfg.Charset,
		}
	}

	dsn := strings.Join(opt, "")

	if cfg.Debug {
		logger.Debug(dsn)
	}

	db, err := sql.Open("mysql", dsn)

	if err != nil {
		defer func() {
			_ = db.Close()
		}()

		logger.Fatal(err)
	}

	return &DB{SQL: db, config: cfg, field: "*"}
}

func New(cfg *Config) *DB {
	cfg.useDb = true

	return Open(cfg)
}

func (db *DB) setParams(i []interface{}) {
	if i != nil {
		db.params.Put(i)
	}
}

func (db *DB) getParams() []interface{} {
	ps, _ := db.params.Get().([]interface{})

	return ps
}

func (db *DB) Configure(k string, v interface{}) *DB {
	switch k {
	case "Prefix":
		db.config.Prefix = v.(string)
	case "Debug":
		db.config.Debug = v.(bool)
	case "Explain":
		db.config.Explain = v.(bool)
	default:
		logger.Error(k + " is invalid argument")
	}

	return db
}

func (db *DB) Table(name string) *DB {
	if strings.HasPrefix(name, db.config.Prefix) {
		db.table = name
	} else {
		db.table = db.config.Prefix + name
	}

	return db
}

func (db *DB) Alias(name string) *DB {
	db.alias = name

	return db
}

func (db *DB) Force(index string) *DB {
	db.force = index

	return db
}

func (db *DB) Field(field interface{}) *DB {
	var fields []string

	switch field.(type) {
	case string:
		if field.(string) == "*" {
			fields = append(fields, field.(string))
		} else {
			fields = append(fields, MakeBackQuote(field.(string), ","))
		}
	case []string:
		for _, v := range field.([]string) {
			if v == "*" {
				fields = append(fields, v)
			} else {
				fields = append(fields, MakeBackQuote(v, ","))
			}
		}
	}

	db.field = strings.Join(fields, ", ")

	return db
}

func (db *DB) Where(w interface{}, andor ...string) *DB {
	var (
		dr  string
		whr []string
		prm []interface{}
	)

	switch len(andor) {
	case 0:
		dr = " and "
	case 1:
		dr = " " + andor[0] + " "
	default:
		logger.Fatal("too many arguments")
	}

	switch w.(type) {
	case string, []string, [][]string, []interface{}:
		db.where = ParseWhere(w, dr, &prm)
	case map[string]interface{}:
		for k, v := range w.(map[string]interface{}) {
			if strings.HasPrefix(k, "and") {
				whr = append(whr, "(" + ParseWhere(v, " and ", &prm) + ")")
			}

			if strings.HasPrefix(k, "or") {
				whr = append(whr, "(" + ParseWhere(v, " or ", &prm) + ")")
			}
		}

		db.where = strings.Join(whr, dr)
	}

	if db.params.New == nil && len(prm) > 0 {
		db.params.New = func() interface{} {
			ps := make([]interface{}, len(prm))
			return &ps
		}
	}

	db.setParams(prm)

	return db
}

func (db *DB) Order(o interface{}) *DB {
	by := func(db *DB, s []string) {
		var order []string

		for i := 0; i < len(s); i++ {
			order = append(order, MakeBackQuote(s[i], " "))
		}

		db.order = strings.Join(order, ", ")
	}

	switch o.(type) {
	case string:
		by(db, strings.Split(o.(string), ","))
	case []string:
		by(db, o.([]string))
	default:
		logger.Fatal("arguments error")
	}

	return db
}

func (db *DB) Limit(l ...interface{}) *DB {
	switch len(l) {
	case 0:
		db.limit = ""
	case 1:
		db.limit = fmt.Sprintf("%v", l[0])
	case 2:
		var limit []string

		for i := 0; i < len(l); i++ {
			if l[i] == "" {
				logger.Fatal("arguments error")
			}

			limit = append(limit, fmt.Sprintf("%v", l[i]))
		}

		db.limit = strings.Join(limit, ", ")
	default:
		logger.Fatal("too many arguments")
	}

	return db
}

func (db *DB) MakeSQL() string {
	var query, force, where, order, limit string

	table := "`" + db.table + "`"

	if db.alias != "" {
		table = table + " " + db.alias
	}

	if db.force != "" {
		force = " FORCE INDEX(" + db.force + ")"
		db.force = ""
	}

	if db.where != "" {
		where = " WHERE " + db.where
		db.where = ""
	}

	if db.order != "" {
		order = " ORDER BY " + db.order
		db.order = ""
	}

	if db.limit != "" {
		limit = " LIMIT " + db.limit
		db.limit = ""
	}

	field := db.field
	db.field = "*"

	query = strings.Join([]string{
		"SELECT ",
		field,
		" FROM ",
		table,
		force,
		where,
		order,
		limit,
	}, "")

	if db.config.Debug {
		logger.Debug(query)
	}

	if db.config.Explain {
		args := db.getParams()
		res  := db.Query("EXPLAIN " + query, args...)

		for i := 0; i < len(res); i++ {
			logger.Debug(ItoS(res[i]))
		}

		db.setParams(args)
	}

	return query
}

func (db *DB) prepare(query string) *sql.Stmt {
	if db.config.Debug {
		logger.Debug(query)
	}

	stmt, err := db.SQL.Prepare(query)
	if err != nil {
		defer func() {
			_ = stmt.Close()
		}()

		logger.Fatal(err)
	}

	return stmt
}

func (db *DB) Prepare(query string) *sql.Stmt {
	db.stmt = db.prepare(query)

	return db.stmt
}

func (db *DB) sqlStmt() *sql.Stmt {
	return db.prepare(db.MakeSQL())
}

func (db *DB) stmtClose() {
	_ = db.stmt.Close()
}

func (db *DB) rowsClose() {
	_ = db.rows.Close()
}

func (db *DB) fetch(args ...interface{}) (fields []string) {
	rows, err := db.stmt.Query(args...)
	defer db.stmtClose()

	db.rows = rows

	if err != nil {
		defer db.rowsClose()
		logger.Fatal(err)
	}

	fields, err = rows.Columns()
	if err != nil {
		defer db.rowsClose()
		logger.Fatal(err)
	}

	return
}

func (db *DB) Fetch() (fields []string) {
	db.stmt = db.sqlStmt()

	return db.fetch(db.getParams()...)
}

func (db *DB) Result(fields []string) (res []interface{}) {
	defer db.rowsClose()

	for db.rows.Next() {
		data := MakeArgs(len(fields))

		err := db.rows.Scan(data...)
		if err != nil {
			logger.Error(err)

			return
		}

		ret := make(map[string]interface{})
		for k, v := range data {
			ret[fields[k]] = v
		}

		res = append(res, ret)
	}

	return
}

func (db *DB) Select() []interface{} {
	return db.Result(db.Fetch())
}

func (db *DB) Find() map[string]interface{} {
	db.limit = "1"

	fields := db.Fetch()

	db.limit = ""

	return db.Result(fields)[0].(map[string]interface{})
}

func (db *DB) Value(field string) string {
	db.field = field
	db.stmt  = db.sqlStmt()
	defer db.stmtClose()

	var res interface{}

	err := db.stmt.QueryRow(db.getParams()...).Scan(&res)
	if err != nil {
		if err == sql.ErrNoRows {
			return "<nil>"
		}

		logger.Fatal(err)
	}

	return ItoS(res)
}

func (db *DB) Count() (num int) {
	db.field = "count(1)"
	db.stmt  = db.sqlStmt()
	defer db.stmtClose()

	err := db.stmt.QueryRow(db.getParams()...).Scan(&num)
	if err != nil {
		if err == sql.ErrNoRows {
			return -1
		}

		logger.Fatal(err)
	}

	return
}

func (db *DB) Query(query string, args ...interface{}) []interface{} {
	db.stmt = db.prepare(query)

	return db.Result(db.fetch(args...))
}

func (db *DB) OneRow(query string, args ...interface{}) map[string]interface{} {
	return db.Query(query, args...)[0].(map[string]interface{})
}

func (db *DB) Exec(query string, args ...interface{}) {
	db.stmt = db.prepare(query)
	defer db.stmtClose()

	res, err := db.stmt.Exec(args...)
	if err != nil {
		logger.Fatal(err)
	}

	db.LastId, _ = res.LastInsertId()
	db.RowNum, _ = res.RowsAffected()
}

func (db *DB) TxExec(query string, args ...interface{}) {
	tx, err := db.SQL.Begin()
	defer func() {
		_ = tx.Rollback()
	}()

	if err != nil {
		logger.Fatal(err)
	}

	db.stmt, err = tx.Prepare(query)
	defer db.stmtClose()

	if err != nil {
		logger.Fatal(err)
	}

	res, err := db.stmt.Exec(args...)
	if err != nil {
		logger.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		logger.Fatal(err)
	}

	db.LastId, _ = res.LastInsertId()
	db.RowNum, _ = res.RowsAffected()
}

func (db *DB) insert(data map[string]interface{}) string {
	var (
		query string
		key   []string
		val   []string
		args  []interface{}
	)

	for k, v := range data {
		key  = append(key, k)
		val  = append(val, "?")
		args = append(args, v)
	}

	_ = db.getParams()
	db.setParams(args)

	query = strings.Join([]string{
		"INSERT INTO `",
		db.table,
		"` (",
		db.Field(key).field,
		") VALUES (",
		strings.Join(val, ", "),
		")",
	}, "")

	if db.config.Debug {
		logger.Debug(query)
	}

	return query
}

func (db *DB) insertGroup(data []map[string]interface{}) (string, [][]interface{}) {
	var (
		key []string
		val []string
	)

	args := make([][]interface{}, len(data))

	for i := 0; i < len(data); i++ {
		var tmp []interface{}

		for k, v := range data[i] {
			if i == 0 {
				key = append(key, k)
				val = append(val, "?")
			}

			tmp = append(tmp, v)
		}

		args[i] = tmp
	}

	query := strings.Join([]string{
		"INSERT INTO `",
		db.table,
		"` (",
		db.Field(key).field,
		") VALUES (",
		strings.Join(val, ", "),
		")",
	}, "")

	if db.config.Debug {
		logger.Debug(query)
	}

	return query, args
}

func (db *DB) update(data map[string]interface{}) string {
	var (
		query string
		key   []string
		val   []interface{}
	)

	for k, v := range data {
		key = append(key, "`"+k+"` = ?")
		val = append(val, v)
	}

	val = append(val, db.getParams()...)
	db.setParams(val)

	query = strings.Join([]string{
		"UPDATE `",
		db.table,
		"` SET ",
		strings.Join(key, ", "),
		func() string {
			if db.where == "" {
				return ""
			} else {
				return " WHERE " + db.where
			}
		}(),
	}, "")

	if db.config.Debug {
		logger.Debug(query)
	}

	return query
}

func (db *DB) updateGroup(data []map[string]interface{}) (string, [][]interface{}) {
	var key []string

	args := make([][]interface{}, len(data))
	tmp  := db.getParams()

	for i := 0; i < len(data); i++ {
		var val []interface{}

		for k, v := range data[i] {
			if i == 0 {
				key = append(key, "`" + k + "` = ?")
			}

			val = append(val, v)

			if db.where != "" {
				val = append(val, tmp...)
			}
		}

		args[i] = val
	}

	query := strings.Join([]string{
		"UPDATE `",
		db.table,
		"` SET ",
		strings.Join(key, ", "),
		func() string {
			if db.where == "" {
				return ""
			} else {
				return " WHERE " + db.where
			}
		}(),
	}, "")

	return query, args
}

func (db *DB) save(data interface{}, handle string) {
	switch data.(type) {
	case map[string]interface{}:
		if handle == "insert" {
			db.Exec(db.insert(data.(map[string]interface{})), db.getParams()...)
		} else if handle == "update" {
			db.Exec(db.update(data.(map[string]interface{})), db.getParams()...)
		} else {
			logger.Fatal("invalid handle:", handle)
		}
	case []map[string]interface{}:
		var (
			query string
			args  [][]interface{}
		)

		if handle == "insert" {
			query, args = db.insertGroup(data.([]map[string]interface{}))
		} else if handle == "update" {
			query, args = db.updateGroup(data.([]map[string]interface{}))
		} else {
			logger.Fatal("invalid handle:", handle)
		}

		db.stmt = db.prepare(query)
		defer db.stmtClose()

		for i := 0; i < len(args); i++ {
			if _, err := db.stmt.Exec(args[i]...); err != nil {
				logger.Fatal(i, err)
			}
		}
	default:
		logger.Fatal("invalid argument")
	}
}

func (db *DB) Insert(data interface{}) {
	db.save(data, "insert")
}

func (db *DB) TxInsert(data map[string]interface{}) {
	db.TxExec(db.insert(data), db.getParams()...)
}

func (db *DB) Update(data interface{}) {
	db.save(data, "update")
}

func (db *DB) TxUpdate(data map[string]interface{}) {
	db.TxExec(db.update(data), db.getParams()...)
}

func (db *DB) delete() string {
	query := strings.Join([]string{
		"DELETE FROM `",
		db.table,
		func() string {
			if db.where == "" {
				_ = db.getParams()
				return "`"
			} else {
				return "` WHERE " + db.where
			}
		}(),
	}, "")

	if db.config.Debug {
		logger.Debug(query)
	}

	return query
}

func (db *DB) Delete() {
	db.Exec(db.delete(), db.getParams()...)
}

func (db *DB) TxDelete() {
	db.TxExec(db.delete(), db.getParams()...)
}

func (db *DB) Use(name string) {
	db.Exec("USE " + name)
}

func (db *DB) Names(charset string) {
	db.Exec("SET NAMES " + charset)
}

func (db *DB) Create(name, charset string) {
	db.Exec(strings.Join([]string{
		"CREATE DATABASE IF NOT EXISTS ",
		name,
		" DEFAULT CHARACTER SET ",
		charset,
		" COLLATE ",
		MakeCharset(charset),
	}, ""))
}

func (db *DB) Drop(name ...string) {
	if len(name) > 1 {
		logger.Fatal("too many arguments")
	} else if len(name) == 1 {
		db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name[0]))
	} else {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", db.table))
	}
}

func (db *DB) Alter(charset string, name ...string) {
	if len(name) > 1 {
		logger.Fatal("too many arguments")
	} else if len(name) == 1 {
		db.Exec(strings.Join([]string{
			"ALTER DATABASE ",
			name[0],
			" CHARACTER SET ",
			charset,
			" COLLATE ",
			MakeCharset(charset),
		}, ""))
	} else {
		db.Exec(strings.Join([]string{
			"ALTER TABLE ",
			db.table,
			" CHARACTER SET ",
			charset,
			" COLLATE ",
			MakeCharset(charset),
		}, ""))
	}
}

func (db *DB) Add(query string) {
	db.Exec(fmt.Sprintf("ALTER TABLE %s ADD %s %s", db.table, db.field, query))
}

func (db *DB) Modify(query string) {
	db.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY %s %s", db.table, db.field, query))
}

func (db *DB) AutoIncrement(id int) {
	db.Exec(fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", db.table, id))
}

func (db *DB) Truncate() {
	db.Exec("TRUNCATE TABLE " + db.table)
}

func (db *DB) Close() {
	_ = db.SQL.Close()
}