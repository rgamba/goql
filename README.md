# goql

[![Build Status](https://travis-ci.org/rgamba/goql.svg?branch=master)](https://travis-ci.org/rgamba/goql)

goql is a super fast and easy to use query builder and database table to struct modeling convention.
It is like an ORM but it just gets out of your way and let's you keep control of your queries.

For the following examples, let's assume you have the following table *user*:

id | username | password
-- | --		  | --
1  | ricardo  | secret123
2  | admin    | ultrasecret

## Mapping a table to a struct

```go
type User struct {
	ID       int64  `db:"id" pk:"true"`
	Username string `db:"username"`
	Password string `db:"password"`
}
```

## Select queries

```go
query := goql.QueryBuilder{}
query.Select("id, username, password").From("user").Where("id = $?", 1).QueryAndScan(db, &user)
fmt.Println(user.username) // -> "ricardo"
```

Or even better...

```go
myuser := User{}
query := goql.QueryBuilder{}
query.Select(myser).Where("id = $?", 1).QueryAndScan(db, &user)
fmt.Println(user.username) // -> "ricardo"
```

## Insert or update

```go
newUser := User{ID: 3, Name: "John", Password: "123"}
Insert(db, "user", newUser)

newUser.Name = "Bob"
Update(db, "user", newUser)
```

*Note* `db` in both cases must be either of type `*sql.DB` or `*sql.Tx`