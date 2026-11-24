package chord

import (
	"encoding/hex"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
)

// Storage keeps one file per KeyID under dir, named by the hex KeyID.
type Storage struct{ dir string }

func NewStorage(dir string) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Storage{dir: dir}, nil
}

func (s *Storage) path(key *big.Int) string { return filepath.Join(s.dir, IDHex(key)) }

func (s *Storage) Put(key *big.Int, data []byte) error {
	tmp, err := os.CreateTemp(s.dir, "put-*.tmp")
	if err != nil {
		return err
	}
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), s.path(key))
}

func (s *Storage) Get(key *big.Int) ([]byte, bool, error) {
	data, err := os.ReadFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}

func (s *Storage) Delete(key *big.Int) error {
	err := os.Remove(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Storage) Keys() ([]*big.Int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	keys := make([]*big.Int, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		raw, err := hex.DecodeString(e.Name())
		if err != nil {
			continue
		}
		keys = append(keys, new(big.Int).SetBytes(raw))
	}
	return keys, nil
}
