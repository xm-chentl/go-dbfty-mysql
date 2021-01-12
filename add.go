package mysql

import (
	"database/sql"

	"github.com/xm-chentl/go-dbfty/grammar"
)

type add struct {
	db      *sql.DB
	data    interface{}
	grammar grammar.IInsert
	uow     *uowOfWork
}

func (a *add) Exec() error {
	// 生成sql、获取参数
	sql, args, err := a.grammar.Generate(a.data)
	if err != nil {
		return err
	}
	if a.uow != nil {
		a.uow.register(sql, args)
		return nil
	}

	_, err = a.db.Exec(sql, args...)
	if err != nil {
		return err
	}

	return nil
}
