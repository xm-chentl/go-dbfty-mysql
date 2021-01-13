package mysql

import "testing"

func Test_ConnectionString(t *testing.T) {
	connStr := "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8"
	opt := Operation{
		Password: "123456",
		DbName:   "test_db",
	}
	if connStr != opt.connStr() {
		t.Fatal("err")
	}
}
