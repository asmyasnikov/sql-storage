package sql

import (
	"database/sql"
	"database/sql/driver"

	"github.com/asmyasnikov/sql-storage/internal"
	"github.com/asmyasnikov/sql-storage/internal/storage"
)

type storageInterface interface {
	Get(key string) (value string, err error)
	Set(key, value string) (err error)
	Keys() ([]string, error)
}

type sqlStorage struct {
	storage storageInterface
}

func (k sqlStorage) Open(name string) (driver.Conn, error) {
	return internal.Conn(k.storage), nil
}

func init() {
	sql.Register("sql-storage", sqlStorage{
		storage: storage.New(),
	})
}
