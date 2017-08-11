# goql

goql is a super fast and easy to use query builder and database table to struct modeling convention.
It is like an ORM but it just gets out of your way and let's you keep control of your queries.

```go
type User struct {
	ID       int64  `db:"id" pk:"true"`
	Username string `db:"username"`
	password string `db:"password"`
}
user := User{}
query := goql.QueryBuilder{}
query.Select(User).From("users").Where("id = $?", 1).QueryAndScan(db, &user)
fmt.Println(user.username)
```