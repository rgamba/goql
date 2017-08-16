package goql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type User struct {
	ID       int64  `db:"id" pk:"true"`
	Username string `db:"username"`
	Password string `db:"password"`
	Email    string
	Total    string `db:"total" sql:"COUNT(col)"`
}

func dbSetup() *sql.DB {
	Testing = true
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		fmt.Printf("%s", err)
	}
	db.Exec(`
		CREATE TABLE user(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username CHAR(255),
			password CHAR(255)
		)`)
	return db
}

func TestSelectWhenPassedString(t *testing.T) {
	expected := `SELECT id FROM mytable`
	qb := QueryBuilder{}
	qb.IgnoreDynamic = true
	qb.Select("id").From("mytable")
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSelectWithStructWhenUsingDbTag(t *testing.T) {
	expected := `SELECT "id","username","password","total" FROM users`
	user := User{}
	qb := QueryBuilder{}
	qb.IgnoreDynamic = true
	qb.Select(user).From("users")
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func testSelectWhenGuessingTableName(t *testing.T) {
	expected := `SELECT "id","username","password","total" FROM user`
	user := User{}
	qb := QueryBuilder{}
	qb.IgnoreDynamic = true
	qb.Select(user)
	qb.Build()

	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSelectWithoutIgnoringDynamic(t *testing.T) {
	expected := `SELECT "id","username","password",(COUNT(col)) "total" FROM users`
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

func TestSimpleWhere(t *testing.T) {
	expected := `SELECT user FROM users WHERE id = $?`
	qb := QueryBuilder{}
	qb.Select("user").From("users").Where("id = $?")
	qb.Build()
	if strings.Trim(qb.Sql, " ") != expected {
		t.Error("Expected: ", expected, " Got: ", qb.Sql)
	}
}

func TestMultipleWhere(t *testing.T) {
	expected := `SELECT user FROM users WHERE user = 'user' AND password = 'secret'`
	qb := QueryBuilder{}
	qb.Select("user").From("users").Where("user = 'user'").Where("password = 'secret'")
	qb.Build()
	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestSimpleInnerJoin(t *testing.T) {
	expected := `SELECT user FROM users INNER JOIN config USING(id)`
	qb := QueryBuilder{}
	qb.Select("user").From("users").InnerJoin("config USING(id)")
	qb.Build()
	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestMulipleInnerJoin(t *testing.T) {
	expected := `SELECT user FROM users INNER JOIN config USING(id) INNER JOIN other USING(other_id)`
	qb := QueryBuilder{}
	qb.Select("user").From("users").InnerJoin("config USING(id)").InnerJoin("other USING(other_id)")
	qb.Build()
	if strings.Trim(qb.Sql, " ") != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, qb.Sql)
	}
}

func TestInsert(t *testing.T) {
	db := dbSetup()
	defer db.Close()
	newuser := User{Username: "test", Password: "123"}
	result, err := Insert(db, "user", newuser)
	if err != nil {
		t.Error("Insert error: ", err)
	}
	if rows, _ := result.RowsAffected(); rows <= 0 {
		t.Error("Insert didn't product any affected rows")
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM user").Scan(&count)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Error("Expected 1 row, got", count)
	}
}

func TestUpdate(t *testing.T) {
	db := dbSetup()
	defer db.Close()

	db.Exec(`INSERT INTO user(username, password) VALUES('john', 'doe')`)
	newuser := User{ID: 1, Username: "NewUser", Password: "NewPassword"}
	result, err := Update(db, "user", newuser)
	if err != nil {
		t.Error(err)
	}
	if rows, _ := result.RowsAffected(); rows <= 0 {
		t.Error("No rows affected by the update")
	}
	var user, password string
	err = db.QueryRow("SELECT username, password FROM user WHERE id = 1").Scan(&user, &password)
	if err != nil {
		t.Error(err)
	}
	if user != "NewUser" {
		t.Errorf("Expected 'NewUser' got '%s'", user)
	}
	if password != "NewPassword" {
		t.Errorf("Expected 'NewPassword' got '%s'", password)
	}
}

func TestDelete(t *testing.T) {
	db := dbSetup()
	defer db.Close()

	db.Exec(`INSERT INTO user(username, password) VALUES('john', 'doe')`)
	newuser := User{ID: 1, Username: "NewUser", Password: "NewPassword"}
	result, err := Delete(db, "user", newuser)
	if err != nil {
		t.Error(err)
	}
	if rows, _ := result.RowsAffected(); rows <= 0 {
		t.Error("No rows affected by the delete")
	}
	var total int
	err = db.QueryRow("SELECT COUNT(*) FROM user WHERE id = 1").Scan(&total)
	if err != nil {
		t.Error(err)
	}
	if total > 0 {
		t.Error("Delete didn't delete the row")
	}
}
