package goql

import (
	"strings"
	"testing"
)

type User struct {
	ID       int64  `db:"id_user" pk:"true"`
	Username string `db:"username"`
	Password string `db:"password"`
	Email    string
	Total    string `db:"total" sql:"COUNT(col)"`
}

func TestSelectWhenPassedString(t *testing.T) {
	expected := `SELECT id_user FROM mytable`
	qb := QueryBuilder{}
	qb.IgnoreDynamic = true
	qb.Select("id_user").From("mytable")
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSelectWithStructWhenUsingDbTag(t *testing.T) {
	expected := `SELECT "id_user","username","password","total" FROM users`
	user := User{}
	qb := QueryBuilder{}
	qb.IgnoreDynamic = true
	qb.Select(user).From("users")
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSelectWithoutIgnoringDynamic(t *testing.T) {
	expected := `SELECT "id_user","username","password",(COUNT(col)) "total" FROM users`
	user := User{}
	qb := QueryBuilder{}
	qb.Select(user).From("users")
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSelectWithoutInvalidStructAsArg(t *testing.T) {
	defer func() {
		if rec := recover(); rec == nil {
			t.Error("Expected to panic")
		}
	}()
	qb := QueryBuilder{}
	qb.Select(123).From("users")
	qb.Build()
	t.Error("Expected to panic")
}
