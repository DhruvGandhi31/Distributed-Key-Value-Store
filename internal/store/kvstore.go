package store

import (
	"bytes"
	"encoding/gob"
	"sort"
	"strings"
	"sync"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *KVStore {
	return &KVStore{data: make(map[string][]byte)}
}

func (kv *KVStore) Apply(entry raft.LogEntry) interface{} {
	if entry.Type != raft.EntryNormal || len(entry.Data) == 0 {
		return nil
	}
	cmd, err := DecodeCommand(entry.Data)
	if err != nil {
		return &ApplyResult{Err: err.Error()}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		prev := kv.data[cmd.Key]
		kv.data[cmd.Key] = cmd.Value
		return &ApplyResult{OK: true, Previous: prev}

	case OpDelete:
		prev, ok := kv.data[cmd.Key]
		delete(kv.data, cmd.Key)
		return &ApplyResult{OK: ok, Previous: prev}

	case OpCAS:
		curr := kv.data[cmd.Key]
		if !bytes.Equal(curr, cmd.Expected) {
			return &ApplyResult{OK: false, Previous: curr}
		}
		kv.data[cmd.Key] = cmd.Value
		return &ApplyResult{OK: true, Previous: curr}
	}
	return nil
}

func (kv *KVStore) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Scan(prefix string) [][2]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var results [][2]string
	for k, v := range kv.data {
		if strings.HasPrefix(k, prefix) {
			results = append(results, [2]string{k, string(v)})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i][0] < results[j][0] })
	return results
}

func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (kv *KVStore) Restore(data []byte) error {
	m := make(map[string][]byte)
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return err
	}
	kv.mu.Lock()
	kv.data = m
	kv.mu.Unlock()
	return nil
}

func (kv *KVStore) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}
