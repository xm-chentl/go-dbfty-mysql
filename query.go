package mysql

import (
	"database/sql"
	"fmt"
	"reflect"

	dbfty "github.com/xm-chentl/go-dbfty"
	"github.com/xm-chentl/go-dbfty/grammar"
	"github.com/xm-chentl/go-dbfty/grammar/aggregation"
	"github.com/xm-chentl/go-dbfty/metadata"
	"github.com/xm-chentl/go-dbfty/utils"
)

type query struct {
	db      *sql.DB
	grammar grammar.ISelect

	sqlByCustom       string
	sqlOfAgrsByCustom []interface{}
}

func (q *query) Order(fields ...string) dbfty.IQuery {
	q.grammar.Query().OrderBy(fields...)

	return q
}

func (q *query) OrderByDesc(fields ...string) dbfty.IQuery {
	q.grammar.Query().OrderByDesc(fields...)

	return q
}

func (q *query) GroupBy(fields ...string) dbfty.IQuery {
	q.grammar.Query().GroupBy(fields...)

	return q
}

func (q *query) Take(num int) dbfty.IQuery {
	q.grammar.Query().Take(num)

	return q
}

func (q *query) Skip(num int) dbfty.IQuery {
	q.grammar.Query().Skip(num)

	return q
}

func (q *query) Where(where string, args ...interface{}) dbfty.IQuery {
	q.grammar.Query().Where(where, args...)

	return q
}

func (q query) Count(entity interface{}) (int, error) {
	sql, args, err := q.grammar.Aggregation(
		aggregation.Count("*"),
	).Generate(entity)
	if err != nil {
		return 0, err
	}

	rows, err := q.db.Query(sql, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var total int
	for rows.Next() {
		if err := rows.Scan(&total); err != nil {
			return 0, err
		}
	}

	return total, nil
}

func (q query) First(entity interface{}) error {
	rt := reflect.TypeOf(entity)
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("r is not ptr")
	}

	rt = rt.Elem()
	rv := reflect.ValueOf(entity).Elem()
	rvs := reflect.New(
		reflect.SliceOf(rt),
	).Elem()
	if err := q.getData(rt, rvs); err != nil {
		return err
	}
	if rvs.Len() > 0 {
		rv.Set(rvs.Index(0))
	}

	return nil
}

func (q query) ToArray(entities interface{}) error {
	rt := reflect.TypeOf(entities)
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("entities is not a pointer")
	}

	rt = rt.Elem()
	rv := reflect.ValueOf(entities).Elem()
	if rt.Kind() != reflect.Array && rt.Kind() != reflect.Slice {
		return fmt.Errorf("entities is not array or slice")
	}

	// 获取数据中元素的实例
	if err := q.getData(utils.GetTypeBySlice(entities), rv); err != nil {
		return err
	}

	return nil
}

func (q query) Exc(entities interface{}, sql string, args ...interface{}) error {
	rt := utils.GetTypeBySlice(entities)
	rv := reflect.New(rt)
	table, err := metadata.Get(rv.Interface())
	if err != nil {
		return err
	}

	rows, err := q.db.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnsOfRow, columnTypesOfRow := q.getRowsStruct(rows)
	bindRvs := make([]interface{}, 0)
	for i, columnOfRow := range columnsOfRow {
		if column, ok := table.GetColumnsByMap()[columnOfRow]; ok {
			// 字段的指针数组
			bindRvs = append(bindRvs, rv.FieldByName(column.GetStruct().Name).Addr().Interface())
		} else {
			bindRvs = append(bindRvs, reflect.New(columnTypesOfRow[i]).Interface())
		}
	}

	results := reflect.MakeSlice(reflect.SliceOf(rv.Type()), 0, 0)
	for rows.Next() {
		if err := rows.Scan(bindRvs...); err != nil {
			return err
		}
		results = reflect.Append(results, rv)
	}
	reflect.ValueOf(entities).Elem().Set(results)

	return nil
}

func (q query) getData(rt reflect.Type, resultsOfRv reflect.Value) error {
	rv := reflect.New(rt).Elem()
	instance := rv.Interface()
	table, err := metadata.Get(instance)
	if err != nil {
		return err
	}

	sql, args, err := q.grammar.Generate(instance)
	if err != nil {
		return err
	}

	rows, err := q.db.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnsOfRow, columnTypesOfRow := q.getRowsStruct(rows)
	bindRvs := make([]interface{}, 0)
	for i, columnOfRow := range columnsOfRow {
		if column, ok := table.GetColumnsByMap()[columnOfRow]; ok {
			// 字段的指针数组
			bindRvs = append(bindRvs, rv.FieldByName(column.GetStruct().Name).Addr().Interface())
		} else {
			bindRvs = append(bindRvs, reflect.New(columnTypesOfRow[i]).Interface())
		}
	}

	results := reflect.MakeSlice(reflect.SliceOf(rv.Type()), 0, 0)
	for rows.Next() {
		if err := rows.Scan(bindRvs...); err != nil {
			return err
		}
		results = reflect.Append(results, rv)
	}
	resultsOfRv.Set(results)

	return nil
}

func (q query) getRowsStruct(rows *sql.Rows) ([]string, []reflect.Type) {
	columns, _ := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()
	types := make([]reflect.Type, 0)
	for _, typeOfRow := range columnTypes {
		types = append(types, typeOfRow.ScanType())
	}

	return columns, types
}
