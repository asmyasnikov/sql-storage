package internal

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	_ setter = (*storageMock)(nil)
)

type storageMock struct {
	value string
}

func (s *storageMock) Set(key, value string) (err error) {
	s.value = value
	return nil
}

func Test_insert(t *testing.T) {
	for _, tt := range []struct {
		data      *storageMock
		query     string
		wantValue string
		wantErr   error
	}{
		{
			data:    &storageMock{},
			query:   "SELECT 1",
			wantErr: errNotMatched,
		},
		{
			data:      &storageMock{},
			query:     "INSERT INTO memtable (id, value) VALUES ('1', '2');",
			wantErr:   nil,
			wantValue: "2",
		},
	} {
		t.Run("", func(t *testing.T) {
			err := insert(tt.data, tt.query)
			require.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				require.Equal(t, tt.wantValue, tt.data.value)
			}
		})
	}
}
