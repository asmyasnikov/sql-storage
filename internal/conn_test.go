package internal

import (
	"github.com/asmyasnikov/sql-storage/internal/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	_ setter = (*singleKeyStorageMock)(nil)
)

type singleKeyStorageMock struct {
	key   string
	value string
}

func (s *singleKeyStorageMock) Get(key string) (value string, err error) {
	if s.key != key {
		return "", storage.ErrNoData
	}
	return s.value, nil
}

func (s *singleKeyStorageMock) Set(key, value string) (err error) {
	if s.key != key {
		panic("only key = '" + s.key + "' supported")
	}
	s.value = value
	return nil
}

func Test_set(t *testing.T) {
	for _, tt := range []struct {
		data      *singleKeyStorageMock
		query     string
		wantValue string
		wantErr   error
	}{
		{
			data:    &singleKeyStorageMock{},
			query:   "GET 1",
			wantErr: errNotMatched,
		},
		{
			data: &singleKeyStorageMock{
				key: "1",
			},
			query:     "SET 1 = 2;",
			wantErr:   nil,
			wantValue: "2",
		},
	} {
		t.Run("", func(t *testing.T) {
			err := set(tt.data, tt.query)
			require.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				require.Equal(t, tt.wantValue, tt.data.value)
			}
		})
	}
}

func Test_get(t *testing.T) {
	for _, tt := range []struct {
		data      *singleKeyStorageMock
		query     string
		wantValue string
		wantErr   error
	}{
		{
			data:    &singleKeyStorageMock{},
			query:   "SET 1=2;",
			wantErr: errNotMatched,
		},
		{
			data: &singleKeyStorageMock{
				key:   "1",
				value: "2",
			},
			query:     "GET 1;",
			wantErr:   nil,
			wantValue: "2",
		},
	} {
		t.Run("", func(t *testing.T) {
			v, err := get(tt.data, tt.query)
			require.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				require.Equal(t, tt.wantValue, v)
			}
		})
	}
}
