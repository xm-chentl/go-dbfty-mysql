package mysql

import (
	"database/sql"
	"fmt"
)

type queueItem struct {
	sql  string
	args []interface{}
}

type uowOfWork struct {
	db    *sql.DB
	queue []queueItem
}

func (u *uowOfWork) register(sql string, args []interface{}) {
	u.queue = append(u.queue, queueItem{
		sql:  sql,
		args: args,
	})
}

func (u *uowOfWork) reset() {
	u.queue = u.queue[0:0]
}

func (u *uowOfWork) Commit() error {
	defer u.reset()

	var err error
	if err = u.db.Ping(); err != nil {
		fmt.Println(err.Error())
	}

	tx, err := u.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, item := range u.queue {
		result, err := tx.Exec(item.sql, item.args...)
		if err != nil {
			break
		}
		rowAffected, err := result.RowsAffected()
		if err != nil {
			break
		}
		if rowAffected != 1 {
			err = fmt.Errorf("exec is fail [%s]", item.sql)
			break
		}
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func newUow(db *sql.DB) *uowOfWork {
	return &uowOfWork{
		db: db,
	}
}
