package kambodja

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

type kambodja struct {
	storage storageInterface
}

func (k kambodja) Open(name string) (driver.Conn, error) {
	return internal.Conn(k.storage), nil
}

func init() {
	sql.Register("kambodja", kambodja{
		storage: storage.New(),
	})
}
