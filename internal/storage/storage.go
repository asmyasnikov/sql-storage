package storage

import (
	"fmt"
	"sync"
)

var (
	_ interface {
		Get(key string) (value string, err error)
		Set(key, value string) (err error)
		Keys() (keys []string, err error)
	} = (*storage)(nil)
)

type storage struct {
	data map[string]string
	mu   sync.RWMutex
}

func New() *storage {
	return &storage{
		data: make(map[string]string),
	}
}

func (s *storage) Get(key string) (value string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.data[key]; ok {
		return v, nil
	}
	return "", fmt.Errorf("nothing value by key=%q", key)
}

func (s *storage) Set(key, value string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == "" {
		delete(s.data, key)
	} else {
		s.data[key] = value
	}
	return nil
}

func (s *storage) Keys() (keys []string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys = make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys, nil
}
