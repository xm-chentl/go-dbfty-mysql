package mysql

import (
	"database/sql"
	"fmt"

	// mysql
	_ "github.com/go-sql-driver/mysql"

	dbfty "github.com/xm-chentl/go-dbfty"
	"github.com/xm-chentl/go-dbfty/grammar"
)

type repository struct {
	readerDb *sql.DB
	writerDb *sql.DB
	uow      *uowOfWork
	grammar  grammar.IGrammar
}

func (m repository) Add(entity interface{}) dbfty.IAdd {
	return &add{
		db:      m.writerDb,
		data:    entity,
		uow:     m.uow,
		grammar: m.grammar.Insert(),
	}
}

func (m repository) Delete(entity interface{}) dbfty.IDelete {
	return &delete{
		db:      m.writerDb,
		data:    entity,
		uow:     m.uow,
		grammar: m.grammar.Delete(),
	}
}

func (m repository) Update(entity interface{}) dbfty.IUpdate {
	return &update{
		db:      m.writerDb,
		data:    entity,
		uow:     m.uow,
		grammar: m.grammar.Update(),
	}
}

func (m repository) Query() dbfty.IQuery {
	return &query{
		db:      m.readerDb,
		grammar: m.grammar.Select(),
	}
}

func (m repository) Ping() (bool, error) {
	var err error
	errs := []string{}
	if m.readerDb != nil {
		if err = m.readerDb.Ping(); err != nil {
			errs = append(errs, "reader")
		}
	}
	if m.writerDb != nil {
		if err = m.writerDb.Ping(); err != nil {
			errs = append(errs, "writer")
		}
	}
	switch len(errs) {
	case 2:
		return false, fmt.Errorf("mysql connection failed")
	case 1:
		return false, fmt.Errorf("sql(%s) connection failed", errs[0])
	}

	return true, err
}
