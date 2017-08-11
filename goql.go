package goql

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"database/sql"
)

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
	qb.Sql = ""
	// SELECT
	if len(qb.columns) > 0 {
		qb.Sql = `SELECT ` + strings.Join(qb.columns, `,`) + ` `
	} else {
		qb.Sql = "SELECT * "
	}
	// FROM
	//frm := strings.Split(qb.from, ".")
	qb.Sql += `FROM ` + qb.from + ` `
	if len(qb.SelectAlias) > 0 {
		qb.Sql += qb.SelectAlias + " "
	}
	// INNER JOIN
	if len(qb.innerJoin) > 0 {
		qb.Sql += "INNER JOIN " + strings.Join(qb.innerJoin, " INNER JOIN ") + " "
	}
	// LEFT JOIN
	if len(qb.leftJoin) > 0 {
		qb.Sql += "LEFT JOIN " + strings.Join(qb.leftJoin, " LEFT JOIN ") + " "
	}
	// WHERE
	if len(qb.where) > 0 {
		qb.Sql += "WHERE " + strings.Join(qb.where, " AND ") + " "
	}
	// GROUP BY
	if len(qb.groupBy) > 0 {
		qb.Sql += "GROUP BY " + strings.Join(qb.groupBy, ", ") + " "
	}
	// HAVING
	if len(qb.having) > 0 {
		qb.Sql += "HAVING " + strings.Join(qb.having, " AND ") + " "
	}
	// ORDER BY
	if len(qb.orderBy) > 0 {
		qb.Sql += "ORDER BY " + strings.Join(qb.orderBy, ", ") + " "
	}
	// LIMIT
	if len(qb.limit) > 0 {
		qb.Sql += "LIMIT " + qb.limit
	}

	vals := qb.GetValues()
	if len(vals) > 0 {
		for i := range vals {
			qb.Sql = strings.Replace(qb.Sql, "$?", fmt.Sprintf("$%d", i+1), 1)
		}
	}

	return qb.Sql
}

// BuildCount is the same as Build() with the difference that
// it ignores the values passed to Select() function and replaces it
// with COUNT(*)
func (qb *QueryBuilder) BuildCount() string {
	qb.Sql = ""
	// SELECT
	qb.Sql = "SELECT COUNT(*) "

	// FROM
	qb.Sql += "FROM " + qb.from + " "
	// WHERE
	if len(qb.where) > 0 {
		qb.Sql += "WHERE " + strings.Join(qb.where, " AND ") + " "
	}
	// GROUP BY
	if len(qb.groupBy) > 0 {
		qb.Sql += "GROUP BY " + strings.Join(qb.groupBy, ", ") + " "
	}
	// HAVING
	if len(qb.having) > 0 {
		qb.Sql += "HAVING " + strings.Join(qb.having, " AND ") + " "
	}

	vals := qb.GetValues()
	if len(vals) > 0 {
		for i := range vals {
			qb.Sql = strings.Replace(qb.Sql, "$?", fmt.Sprintf("$%d", i+1), 1)
		}
	}

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
