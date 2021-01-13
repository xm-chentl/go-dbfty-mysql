package mysql

import (
	"database/sql"
	"fmt"

	dbfty "github.com/xm-chentl/go-dbfty"
	"github.com/xm-chentl/go-dbfty/grammar/sql/mysql"
)

//DRIVERNAME 获取名
const DRIVERNAME = "mysql"

type Operation struct {
	Id       string
	Host     string
	Port     uint16
	DbName   string
	User     string
	Password string
}

func (o *Operation) connStr() string {
	if o.Host == "" {
		o.Host = "127.0.0.1"
	}
	if o.Port == 0 {
		o.Port = 3306
	}
	if o.User == "" {
		o.User = "root"
	}

	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8",
		o.User,
		o.Password,
		o.Host,
		o.Port,
		o.DbName,
	)
}

type factory struct {
	isProxy      bool
	readerOfConn string
	writerOfConn string

	repository *repository
}

func (m *factory) Db() dbfty.IRepository {
	return m.getRepository()
}

func (m *factory) Uow() dbfty.IUnitOfWork {
	repository := m.getRepository()
	if repository.uow == nil {
		m.repository.uow = newUow(m.repository.writerDb)
	}

	return m.repository.uow
}

func (m *factory) getRepository() *repository {
	if m.repository == nil {
		readerDb, err := sql.Open(DRIVERNAME, m.readerOfConn)
		if err != nil {
			panic(err)
		}
		m.repository = &repository{
			readerDb: readerDb,
			writerDb: readerDb,
			grammar:  mysql.New(),
		}
		if m.isProxy {
			writerDb, err := sql.Open(DRIVERNAME, m.writerOfConn)
			if err != nil {
				panic(err)
			}
			m.repository.writerDb = writerDb
		}
	}

	return m.repository
}

func ConnectionString(opt Operation) string {
	return opt.connStr()
}

// Proxy 获取一个mysql实例
func Proxy(readConn, writeConn string) dbfty.IFactory {
	return &factory{
		isProxy:      true,
		readerOfConn: readConn,
		writerOfConn: writeConn,
	}
}

// New 默认实例
func New(connStr string) dbfty.IFactory {
	return &factory{
		readerOfConn: connStr,
	}
}
