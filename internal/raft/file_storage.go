package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const hardStateFileName = "hardstate"

// FileStorage is a Storage implementation backed by a single hard-state
// file per node, written atomically (write-tmp, fsync, rename) so a crash
// mid-write never leaves a partially-written file behind.
type FileStorage struct {
	mu sync.Mutex

	dir string

	term          uint64
	votedFor      NodeID
	snapshotIndex uint64
	snapshotTerm  uint64
}

// NewFileStorage opens (or creates) the on-disk hard state for node id
// under dataDir/id. If no hard state file exists yet, it starts fresh with
// zero-valued state.
func NewFileStorage(dataDir string, id NodeID) (*FileStorage, error) {
	dir := filepath.Join(dataDir, string(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("raft: create data dir %s: %w", dir, err)
	}

	fs := &FileStorage{dir: dir}

	path := filepath.Join(dir, hardStateFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fs, nil
		}
		return nil, fmt.Errorf("raft: open hard state %s: %w", path, err)
	}
	defer f.Close()

	if err := fs.decode(f); err != nil {
		return nil, fmt.Errorf("raft: decode hard state %s: %w", path, err)
	}
	return fs, nil
}

func (fs *FileStorage) decode(r io.Reader) error {
	br := bufio.NewReader(r)

	var term uint64
	if err := binary.Read(br, binary.BigEndian, &term); err != nil {
		return err
	}

	var votedForLen uint16
	if err := binary.Read(br, binary.BigEndian, &votedForLen); err != nil {
		return err
	}

	votedForBytes := make([]byte, votedForLen)
	if votedForLen > 0 {
		if _, err := io.ReadFull(br, votedForBytes); err != nil {
			return err
		}
	}

	var snapshotIndex, snapshotTerm uint64
	if err := binary.Read(br, binary.BigEndian, &snapshotIndex); err != nil {
		return err
	}
	if err := binary.Read(br, binary.BigEndian, &snapshotTerm); err != nil {
		return err
	}

	fs.term = term
	fs.votedFor = NodeID(votedForBytes)
	fs.snapshotIndex = snapshotIndex
	fs.snapshotTerm = snapshotTerm
	return nil
}

// SaveHardState persists term/votedFor/snapshot metadata durably before
// returning. The write is fsync'd before the temp file is renamed into
// place, and the rename itself is atomic — a crash can never observe a
// partially-written hard state file.
func (fs *FileStorage) SaveHardState(term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path := filepath.Join(fs.dir, hardStateFileName)
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raft: open tmp hard state %s: %w", tmpPath, err)
	}

	votedForBytes := []byte(votedFor)
	if err := binary.Write(f, binary.BigEndian, term); err != nil {
		f.Close()
		return fmt.Errorf("raft: write term: %w", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint16(len(votedForBytes))); err != nil {
		f.Close()
		return fmt.Errorf("raft: write votedFor length: %w", err)
	}
	if len(votedForBytes) > 0 {
		if _, err := f.Write(votedForBytes); err != nil {
			f.Close()
			return fmt.Errorf("raft: write votedFor: %w", err)
		}
	}
	if err := binary.Write(f, binary.BigEndian, snapshotIndex); err != nil {
		f.Close()
		return fmt.Errorf("raft: write snapshotIndex: %w", err)
	}
	if err := binary.Write(f, binary.BigEndian, snapshotTerm); err != nil {
		f.Close()
		return fmt.Errorf("raft: write snapshotTerm: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsync hard state %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close hard state %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("raft: rename hard state into place: %w", err)
	}

	fs.term = term
	fs.votedFor = votedFor
	fs.snapshotIndex = snapshotIndex
	fs.snapshotTerm = snapshotTerm
	return nil
}

// LastIndex returns the index of the last log entry.
// TODO(Phase 3/4): no real log exists yet, so this always returns 0.
func (fs *FileStorage) LastIndex() uint64 { return 0 }

// LastTerm returns the term of the last log entry.
// TODO(Phase 3/4): no real log exists yet, so this always returns 0.
func (fs *FileStorage) LastTerm() uint64 { return 0 }

// InitialHardState is a plain accessor used at node startup to seed
// in-memory Raft state from whatever was loaded off disk. It is not part
// of the Storage interface.
func (fs *FileStorage) InitialHardState() (term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.term, fs.votedFor, fs.snapshotIndex, fs.snapshotTerm
}
