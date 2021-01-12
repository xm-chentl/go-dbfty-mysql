package mysql

import (
	"database/sql"

	dbfty "github.com/xm-chentl/go-dbfty"
	"github.com/xm-chentl/go-dbfty/grammar"
)

type update struct {
	db      *sql.DB
	data    interface{}
	grammar grammar.IUpdate
	uow     *uowOfWork
}

func (u *update) Set(fields ...string) dbfty.IUpdate {
	u.grammar.Set(fields...)

	return u
}

func (u *update) Where(where string, args ...interface{}) dbfty.IUpdate {
	u.grammar.Query().Where(where, args...)

	return u
}

func (u update) Exec() error {
	sql, args, err := u.grammar.Generate(u.data)
	if err != nil {
		return err
	}
	if u.uow != nil {
		u.uow.register(sql, args)
		return nil
	}

	if _, err = u.db.Exec(sql, args...); err != nil {
		return err
	}

	return nil
}
