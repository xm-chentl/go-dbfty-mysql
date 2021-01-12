package mysql

import (
	"database/sql"

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
