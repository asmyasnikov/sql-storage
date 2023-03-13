package memstorage

import (
	"database/sql"
	"database/sql/driver"
	"github.com/asmyasnikov/sql-storage/internal/xsql"

	"github.com/asmyasnikov/sql-storage/internal/storage"
)

type memtable interface {
	Get(key string) (value string, err error)
	Set(key, value string) (err error)
	Keys() (keys []string, err error)
}

type azazaDriver struct {
	memtable memtable
}

func (a azazaDriver) Open(_ string) (driver.Conn, error) {
	return xsql.New(a.memtable), nil
}

func init() {
	sql.Register("azaza", &azazaDriver{
		memtable: storage.New(),
	})
}
