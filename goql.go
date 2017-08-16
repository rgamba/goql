package goql

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"database/sql"
)

// Testing is a simple testing flag.
var Testing = false

const dbTypeDb = "db"
const dbTypeTx = "tx"

// QueryBuilder is the main structure.
type QueryBuilder struct {
	Sql string
	// The select by struct will add qb alias to the sql id added
	SelectAlias string
	// If set to true, the select will ignore fields with sql tag
	IgnoreDynamic bool

	columns   []string
	where     []string
	having    []string
	orderBy   []string
	limit     string
	groupBy   []string
	innerJoin []string
	leftJoin  []string
	from      string
	values    map[string][]interface{}
}

// Select selects the columns of the query
// col parameter must be either a string or a struct
// with at least one parameter with the "db" tag set
func (qb *QueryBuilder) Select(col interface{}) (ret *QueryBuilder) {
	ret = qb
	switch reflect.TypeOf(col).Kind() {
	case reflect.String:
		// Passed in as a string
		if qb.columns == nil {
			qb.columns = []string{}
		}
		qb.columns = append(qb.columns, col.(string))
	case reflect.Struct:
		// Passed in a a structure
		t := reflect.TypeOf(col)
		qb.From(qb.guessTableNameFromStruct(t.Name()))
		cols := []string{}
		// Loops all fields
		for i := 0; i <= t.NumField()-1; i++ {
			if name := t.Field(i).Tag.Get("db"); name != "" {
				tSql := t.Field(i).Tag.Get("sql")
				if len(tSql) > 0 && !qb.IgnoreDynamic {
					name = fmt.Sprintf(`(%s) "%s"`, tSql, name)
				} else {
					prefix := t.Field(i).Tag.Get("prefix")
					if len(prefix) <= 0 {
						prefix = qb.SelectAlias
					}
					if len(prefix) > 0 {
						name = fmt.Sprintf(`"%s"."%s"`, prefix, name)
					} else {
						name = fmt.Sprintf(`"%s"`, name)
					}
				}
				cols = append(cols, name)
			}
		}
		// Validate if we have at leat 1 field or panic
		if len(cols) <= 0 {
			panic("The structure has no db fields to select")
		}
		// All good
		for _, v := range cols {
			qb.columns = append(qb.columns, v)
		}
	default:
		// All other types are unsupported
		panic("Unsupported interface passed")
	}
	return
}

func (qb *QueryBuilder) guessTableNameFromStruct(name string) string {
	return strings.ToLower(name)
}

// From tells the compiler where to load the results from (table name)
func (qb *QueryBuilder) From(from string) (ret *QueryBuilder) {
	ret = qb
	qb.from = from
	return
}

// InnerJoin is used if we want to user SQL joins
// Can be used multiple times each one for each join
func (qb *QueryBuilder) InnerJoin(from string) (ret *QueryBuilder) {
	ret = qb
	qb.innerJoin = append(qb.innerJoin, from)
	return
}

// LeftJoin for building left joins
func (qb *QueryBuilder) LeftJoin(from string) (ret *QueryBuilder) {
	ret = qb
	qb.leftJoin = append(qb.leftJoin, from)
	return
}

// Where for filtering using WHERE sql statement
// Can be used multiple times for multiple filters
// IMPORTANT: wilcards MUST be passed as $? in the where string, for example:
// queryBuilder.Where("id = $?", myId)
func (qb *QueryBuilder) Where(where string, vals ...interface{}) (ret *QueryBuilder) {
	ret = qb
	if qb.where == nil {
		qb.where = []string{}
	}
	qb.where = append(qb.where, where)
	if vals != nil {
		if qb.values == nil {
			qb.values = map[string][]interface{}{}
		}
		if _, ok := qb.values["where"]; !ok {
			qb.values["where"] = vals
		} else {
			for _, v := range vals {
				qb.values["where"] = append(qb.values["where"], v)
			}

		}
	}
	return
}

// Having performs having SQL statement
func (qb *QueryBuilder) Having(having string) (ret *QueryBuilder) {
	ret = qb
	if qb.having == nil {
		qb.having = []string{}
	}
	qb.having = append(qb.having, having)
	return
}

// OrderBy for SQL ORDER BY
func (qb *QueryBuilder) OrderBy(order string) (ret *QueryBuilder) {
	ret = qb
	if qb.orderBy == nil {
		qb.orderBy = []string{}
	}
	qb.orderBy = append(qb.orderBy, order)
	return
}

// GroupBy for SQL GROUP BY
func (qb *QueryBuilder) GroupBy(group string) (ret *QueryBuilder) {
	ret = qb
	if qb.groupBy == nil {
		qb.groupBy = []string{}
	}
	qb.groupBy = append(qb.groupBy, group)
	return
}

// Limit is used for LIMIT SQL query
func (qb *QueryBuilder) Limit(limit string) (ret *QueryBuilder) {
	ret = qb
	qb.limit = limit
	return
}

// GetValues gets the values passed to Where() in the second
// parameter. qb is used when building the query, for example:
// queryBuilder.Select("name").From("user").Where("id_user = $?", id)
// DB.QueryRow(queryBuilder.Build(), queryBuilder.GetValues()...)
func (qb *QueryBuilder) GetValues() []interface{} {
	ret := []interface{}{}
	if _, ok := qb.values["where"]; ok {
		for _, v := range qb.values["where"] {
			ret = append(ret, v)
		}

	}
	return ret
}

// Build generates the resulting SQL of the query builder
func (qb *QueryBuilder) Build() string {
	qb.Sql = qb.buildSQL()
	qb.replaceWhereValues()
	return qb.Sql
}

func (qb *QueryBuilder) replaceWhereValues() {
	vals := qb.GetValues()
	if len(vals) > 0 {
		for i := range vals {
			qb.Sql = strings.Replace(qb.Sql, getPlaceholder(), getPlaceholderWithCounter(i+1), 1)
		}
	}
}

func (qb *QueryBuilder) buildSQL() string {
	parts := []string{
		qb.buildSelect(),
		qb.buildFrom(),
		qb.buildInnerJoin(),
		qb.buildLeftJoin(),
		qb.buildWhere(),
		qb.buildGroupBy(),
		qb.buildHaving(),
		qb.buildOrderBy(),
		qb.buildLimit(),
	}
	parts = reduceEmptyElements(parts)
	return strings.Join(parts, " ")
}

func (qb *QueryBuilder) buildCountSQL() string {
	parts := []string{
		"SELECT COUNT(*)",
		qb.buildFrom(),
		qb.buildInnerJoin(),
		qb.buildLeftJoin(),
		qb.buildWhere(),
		qb.buildGroupBy(),
		qb.buildHaving(),
		qb.buildOrderBy(),
		qb.buildLimit(),
	}
	parts = reduceEmptyElements(parts)
	return strings.Join(parts, " ")
}

func (qb *QueryBuilder) buildSelect() string {
	if len(qb.columns) > 0 {
		return `SELECT ` + strings.Join(qb.columns, `,`)
	}
	return "SELECT * "
}

func (qb *QueryBuilder) buildFrom() string {
	result := `FROM ` + qb.from
	if len(qb.SelectAlias) > 0 {
		result += " " + qb.SelectAlias
	}
	return result
}

func (qb *QueryBuilder) buildInnerJoin() string {
	if len(qb.innerJoin) > 0 {
		return "INNER JOIN " + strings.Join(qb.innerJoin, " INNER JOIN ")
	}
	return ""
}

func (qb *QueryBuilder) buildLeftJoin() string {
	if len(qb.leftJoin) > 0 {
		return "LEFT JOIN " + strings.Join(qb.leftJoin, " LEFT JOIN ")
	}
	return ""
}

func (qb *QueryBuilder) buildWhere() string {
	if len(qb.where) > 0 {
		return "WHERE " + strings.Join(qb.where, " AND ")
	}
	return ""
}

func (qb *QueryBuilder) buildGroupBy() string {
	if len(qb.groupBy) > 0 {
		return "GROUP BY " + strings.Join(qb.groupBy, ", ")
	}
	return ""
}

func (qb *QueryBuilder) buildHaving() string {
	if len(qb.having) > 0 {
		return "HAVING " + strings.Join(qb.having, " AND ")
	}
	return ""
}

func (qb *QueryBuilder) buildOrderBy() string {
	if len(qb.orderBy) > 0 {
		return "ORDER BY " + strings.Join(qb.orderBy, ", ")
	}
	return ""
}

func (qb *QueryBuilder) buildLimit() string {
	if len(qb.limit) > 0 {
		return "LIMIT " + qb.limit
	}
	return ""
}

// BuildCount is the same as Build() with the difference that
// it ignores the values passed to Select() function and replaces it
// with COUNT(*)
func (qb *QueryBuilder) BuildCount() string {
	qb.Sql = qb.buildCountSQL()
	qb.replaceWhereValues()
	return qb.Sql
}

// Query is a shortcut for building the query, passing it to the DB driver
// and passing it the values
func (qb *QueryBuilder) Query(Db *sql.DB) (*sql.Rows, error) {
	return Db.Query(qb.Build(), qb.GetValues()...)
}

// QueryAndScan is used for executing a query and scanning it's result
// into the struct's parameters passed in obj.
func (qb *QueryBuilder) QueryAndScan(Db *sql.DB, obj interface{}) error {
	sql := qb.Build()
	vals := qb.GetValues()
	pointers := GetFieldPointers(obj)
	err := Db.QueryRow(sql, vals...).Scan(pointers...)
	if err != nil {
		log.Println(err)
	}
	return err
}

// GetFieldPointers is used to get the pointer position for
// the mapped parameters in a struct, useful for passing these pointers
// to a scanner function such as Db.Scan(db.GetFieldPointers(&a)...)
// NOTE that obj must be a pointer to the structure
func GetFieldPointers(obj interface{}) []interface{} {
	t := reflect.TypeOf(obj).Elem()
	v := reflect.ValueOf(obj).Elem()
	fields := []interface{}{}
	// Loops all fields
	for i := 0; i <= v.NumField(); i++ {
		if len(t.Field(i).Tag.Get("db")) > 0 {
			fields = append(fields, v.Field(i).Addr().Interface())
		}
	}
	return fields
}

// QueryStructInfo represents a parsed information that
// holds metadata of the object after parsing tags, position of
// each field and actual values of the structure in each field.
type QueryStructInfo struct {
	Positions        []string
	Fields           []string
	FieldsForUpdate  []string
	Values           []interface{}
	PrimaryKeys      string
	PrimaryKeyQuery  []string
	PrimaryKeyValues []interface{}
}

// Insert inserts a new record in a table
// The fields in the structure obj must be added the
// "db" tag in the declaration of the structure
func Insert(Db interface{}, table string, obj interface{}) (sql.Result, error) {
	dbType := getDbType(Db)

	queryInfo, err := creatQueryStructInfo(obj)
	if err != nil {
		return nil, err
	}

	// Build the query
	qry := fmt.Sprintf(`INSERT INTO %s ("%s") VALUES(%s)`, table, strings.Join(queryInfo.Fields, `","`), strings.Join(queryInfo.Positions, ","))
	err = nil

	if dbType == dbTypeDb {
		return Db.(*sql.DB).Exec(qry, queryInfo.Values...)
	}
	return Db.(*sql.Tx).Exec(qry, queryInfo.Values...)
}

// Update updates a record. Note that this only works for atomic updates
// and not for massive updates. The field with primary tag will serve as
// update reference, in case there is no field with primary, the update will fail
func Update(Db interface{}, table string, obj interface{}) (sql.Result, error) {
	dbType := getDbType(Db)

	queryInfo, err := creatQueryStructInfo(obj)
	if err != nil {
		return nil, err
	}

	if len(queryInfo.PrimaryKeyQuery) <= 0 {
		return nil, errors.New("there is no primary key in the structure")
	}

	// Build the query
	qry := fmt.Sprintf(`UPDATE %s SET %s WHERE (%s)`, table, strings.Join(queryInfo.FieldsForUpdate, `,`), strings.Join(queryInfo.PrimaryKeyQuery, ` AND `))
	values := append(queryInfo.Values, queryInfo.PrimaryKeyValues...)
	if dbType == dbTypeDb {
		return Db.(*sql.DB).Exec(qry, values...)
	}
	return Db.(*sql.Tx).Exec(qry, values...)
}

// Delete function deletes the structure based on the pk tag of the attribute
func Delete(Db interface{}, table string, obj interface{}) (sql.Result, error) {
	dbType := getDbType(Db)

	queryInfo, err := creatQueryStructInfo(obj)
	if err != nil {
		return nil, err
	}

	if len(queryInfo.PrimaryKeyQuery) <= 0 {
		return nil, errors.New("There is no primary key in the structure")
	}
	qry := fmt.Sprintf(`DELETE FROM %s WHERE (%s)`, table, strings.Join(queryInfo.PrimaryKeyQuery, ","))

	if dbType == dbTypeDb {
		return Db.(*sql.DB).Exec(qry, queryInfo.PrimaryKeyValues...)
	}
	return Db.(*sql.Tx).Exec(qry, queryInfo.PrimaryKeyValues...)
}

// Helpers

func reduceEmptyElements(items []string) []string {
	result := []string{}
	for _, text := range items {
		if strings.Trim(text, " ") != "" {
			result = append(result, text)
		}
	}
	return result
}

func getPlaceholderWithCounter(i int) string {
	if Testing {
		return "?"
	}
	return fmt.Sprintf("$%d", i)
}

func getPlaceholder() string {
	if Testing {
		return "?"
	}
	return "$?"
}

func getDbType(Db interface{}) string {
	switch Db.(type) {
	case *sql.DB:
		return dbTypeDb
	case *sql.Tx:
		return dbTypeTx
	default:
		panic("invalid db type struct")
	}
}

func creatQueryStructInfo(obj interface{}) (*QueryStructInfo, error) {
	result := QueryStructInfo{}

	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	num := t.NumField()
	var err error

	if num <= 0 {
		return nil, errors.New("obj has no properties")
	}

	j := 1
	for i := 0; i <= num-1; i++ {
		fType := t.Field(i)
		fVal := v.Field(i)
		// Check if the field is calculated
		if len(fType.Tag.Get("sql")) > 0 {
			continue
		}
		if len(fType.Tag.Get("pk")) > 0 {
			result.PrimaryKeyQuery = append(result.PrimaryKeyQuery, fmt.Sprintf(`"%s" = %s`, fType.Tag.Get("db"), getPlaceholderWithCounter(j)))
			result.PrimaryKeys = fType.Tag.Get("db")
			result.PrimaryKeyValues = append(result.PrimaryKeyValues, fVal.Interface())
			continue
		}
		// Check for the database field tag
		if len(fType.Tag.Get("db")) <= 0 {
			continue
		}
		if len(fType.Tag.Get("pk")) <= 0 {
			result.FieldsForUpdate = append(result.FieldsForUpdate, fmt.Sprintf(`"%s" = %s`, fType.Tag.Get("db"), getPlaceholderWithCounter(j)))
		}
		// Special tags
		var appendVal interface{}
		switch fType.Tag.Get("type") {
		case "time":
			tme, ok := fVal.Interface().(time.Time)
			if ok {
				appendVal = tme.Format("15:04:05")
			}
		case "json":
			var m interface{}
			if fVal.Interface() == nil {
				m = nil
			} else {
				m, err = json.Marshal(fVal.Interface())
			}
			if err == nil {
				appendVal = m
			}
		default:
			appendVal = fVal.Interface()
		}
		result.Values = append(result.Values, appendVal)
		result.Fields = append(result.Fields, fType.Tag.Get("db"))

		result.Positions = append(result.Positions, getPlaceholderWithCounter(j))
		j++
	}

	return &result, nil
}
