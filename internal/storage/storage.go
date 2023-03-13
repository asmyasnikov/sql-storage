package storage

import (
	"errors"
	"fmt"
	"github.com/asmyasnikov/sql-storage/internal/xerrors"
	"sync"
)

var (
	ErrNoData = errors.New("no data")

	_ Storage = (*storage)(nil)
)

type Storage interface {
	Get(key string) (value string, err error)
	Set(key, value string) (err error)
	Keys() ([]string, error)
}

func New() *storage {
	return &storage{
		data: make(map[string]string),
	}
}

type storage struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *storage) Get(key string) (value string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, is := s.data[key]; is {
		return v, nil
	}
	return "", xerrors.WithStackTrace(fmt.Errorf("%w (key = %q)", ErrNoData, key))
}

func (s *storage) Set(key, value string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *storage) Keys() (keys []string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *storage) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make(map[string]string)
	for k, v := range s.data {
		data[k] = v
	}
	return s.data
}
