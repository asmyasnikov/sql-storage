package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/asmyasnikov/sql-storage/internal/xsql"
	"os/signal"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/asmyasnikov/sql-storage"
)

func TestAzazaDriver(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	db, err := sql.Open("azaza", "")
	require.NoError(t, err)
	t.Run("UPSERT", func(t *testing.T) {
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('1234', '5678');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key', 'value');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key1', 'value1');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key2', 'value2');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key3', 'value3');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key4', 'value4');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPSERT INTO memtable (id, value) VALUES ('key5', 'value5');")
		require.NoError(t, err)
	})
	t.Run("FULL-SCAN", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT * FROM memtable;")
		require.NoError(t, err)
		for rows.Next() {
			var id, value string
			err = rows.Scan(&id, &value)
			require.NoError(t, err)
			fmt.Printf("%s, %s\n", id, value)
		}
		err = rows.Err()
		require.NoError(t, err)
	})
	t.Run("LOOKUP", func(t *testing.T) {
		row := db.QueryRowContext(ctx, "SELECT value FROM memtable WHERE id='123';")
		require.Error(t, row.Err())
		row = db.QueryRowContext(ctx, "SELECT value FROM memtable WHERE id='1234';")
		require.NoError(t, row.Err())
		var value string
		err = row.Scan(&value)
		require.NoError(t, err)
		fmt.Printf("value(1234) = %s\n", value)
		err = row.Err()
		require.NoError(t, err)
	})
	t.Run("ERRORS", func(t *testing.T) {
		_, err := db.QueryContext(ctx, "DELETE FROM memtable;")
		require.Error(t, err)
		if errors.Is(err, xsql.ErrUnknownQuery) {
			fmt.Printf("err = %v is xsql.ErrUnknownQuery\n", err)
		}
		var someErr interface {
			error
			DoSome() string
		}
		if errors.As(err, &someErr) {
			fmt.Printf("err = %v is someErr: %s\n", someErr.DoSome())
		}
	})
	err = db.Close()
	require.NoError(t, err)
}
