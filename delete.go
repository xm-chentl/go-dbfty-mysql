package mysql

import (
	"database/sql"

	dbfty "github.com/xm-chentl/go-dbfty"
	"github.com/xm-chentl/go-dbfty/grammar"
)

type delete struct {
	db      *sql.DB
	data    interface{}
	grammar grammar.IDelete
	uow     *uowOfWork
}

func (d *delete) Where(sql string, args ...interface{}) dbfty.IDelete {
	d.grammar.Query().Where(sql, args...)

	return d
}

func (d delete) Exec() error {
	sql, args, err := d.grammar.Generate(d.data)
	if err != nil {
		return err
	}
	if d.uow != nil {
		d.uow.register(sql, args)
		return nil
	}

	if _, err = d.db.Exec(sql, args...); err != nil {
		return err
	}

	return nil
}
