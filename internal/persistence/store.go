package persistence

import "distributed-kv-store/internal/store"

// Store combines the in-memory engine with a write-ahead log.
type Store struct {
	mem *store.Store
	wal *WAL
}

// OpenStore creates a durable store and restores state from disk.
func OpenStore(path string) (*Store, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	s := &Store{
		mem: store.New(),
		wal: wal,
	}

	if err := wal.Replay(s.applyRecord); err != nil {
		_ = wal.Close()
		return nil, err
	}

	return s, nil
}

// Set writes to the WAL first, then updates memory.
func (s *Store) Set(key, value string) error {
	record := Record{
		Command: CommandSet,
		Key:     key,
		Value:   value,
	}

	if err := s.wal.Append(record); err != nil {
		return err
	}

	s.mem.Set(key, value)
	return nil
}

// Get returns the current value for a key.
func (s *Store) Get(key string) (string, bool) {
	return s.mem.Get(key)
}

// Delete writes the delete intent to the WAL and removes the key in memory.
func (s *Store) Delete(key string) (bool, error) {
	if _, ok := s.mem.Get(key); !ok {
		return false, nil
	}

	record := Record{
		Command: CommandDelete,
		Key:     key,
	}

	if err := s.wal.Append(record); err != nil {
		return false, err
	}

	s.mem.Delete(key)
	return true, nil
}

// Close closes the WAL file.
func (s *Store) Close() error {
	return s.wal.Close()
}

func (s *Store) applyRecord(record Record) error {
	switch record.Command {
	case CommandSet:
		s.mem.Set(record.Key, record.Value)
	case CommandDelete:
		s.mem.Delete(record.Key)
	default:
		return errUnsupportedCommand
	}

	return nil
}
