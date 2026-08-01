package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	hardStateFileName = "hardstate"
	logFileName       = "log"
)

// FileStorage is a Storage implementation backed by two files per node: a
// hard-state file (written atomically: write-tmp, fsync, rename) and an
// append-only log file (each entry length-prefixed and gob-encoded, fsync'd
// after every append). A crash mid-write can never leave a partially-applied
// hard-state change behind, and can at worst leave one torn trailing log
// entry, which NewFileStorage discards on recovery.
//
// The full log is also kept in memory (entries), since Phase 5 hasn't
// introduced compaction yet and the logs this project deals with are small.
// FirstIndex is always 1 until then.
type FileStorage struct {
	mu sync.Mutex

	dir     string
	logPath string

	term          uint64
	votedFor      NodeID
	snapshotIndex uint64
	snapshotTerm  uint64

	// entries[i] is the log entry at index i+1 (FirstIndex is always 1).
	entries []LogEntry
}

// NewFileStorage opens (or creates) the on-disk state for node id under
// dataDir/id. If no hard-state or log file exists yet, it starts fresh with
// zero-valued state and an empty log.
func NewFileStorage(dataDir string, id NodeID) (*FileStorage, error) {
	dir := filepath.Join(dataDir, string(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("raft: create data dir %s: %w", dir, err)
	}

	fs := &FileStorage{dir: dir, logPath: filepath.Join(dir, logFileName)}

	hsPath := filepath.Join(dir, hardStateFileName)
	if f, err := os.Open(hsPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("raft: open hard state %s: %w", hsPath, err)
		}
	} else {
		err := fs.decodeHardState(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("raft: decode hard state %s: %w", hsPath, err)
		}
	}

	if err := fs.loadLog(); err != nil {
		return nil, fmt.Errorf("raft: load log %s: %w", fs.logPath, err)
	}

	return fs, nil
}

func (fs *FileStorage) decodeHardState(r io.Reader) error {
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

// loadLog scans the log file from the beginning, decoding each
// length-prefixed gob entry. A truncated trailing record (a crash mid-write
// left a length prefix with fewer than `len` bytes following it, or fewer
// than 4 bytes for the prefix itself) is treated as the end of the log, not
// an error — everything durably written before it is still valid.
func (fs *FileStorage) loadLog() error {
	f, err := os.Open(fs.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var entries []LogEntry
	for {
		var length uint32
		if err := binary.Read(br, binary.BigEndian, &length); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(br, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // torn trailing write; stop here
			}
			return err
		}

		var entry LogEntry
		if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&entry); err != nil {
			break // torn/corrupt trailing record; stop here
		}
		entries = append(entries, entry)
	}

	fs.entries = entries
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

// AppendEntries durably appends entries to the log file, fsyncing once
// after writing all of them, then updates the in-memory tail. The log file
// is opened fresh for each call (rather than holding a long-lived handle)
// so that TruncateAfter's rewrite-and-rename is always followed by a clean
// re-open, with no stale file handle left pointing at the old inode.
func (fs *FileStorage) AppendEntries(newEntries []LogEntry) error {
	if len(newEntries) == 0 {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.OpenFile(fs.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("raft: open log %s: %w", fs.logPath, err)
	}
	defer f.Close()

	for _, entry := range newEntries {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
			return fmt.Errorf("raft: encode log entry %d: %w", entry.Index, err)
		}
		if err := binary.Write(f, binary.BigEndian, uint32(buf.Len())); err != nil {
			return fmt.Errorf("raft: write log entry length: %w", err)
		}
		if _, err := f.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("raft: write log entry %d: %w", entry.Index, err)
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("raft: fsync log %s: %w", fs.logPath, err)
	}

	fs.entries = append(fs.entries, newEntries...)
	return nil
}

// Entries returns the entries in [lo, hi), clamped to the log's actual
// bounds.
func (fs *FileStorage) Entries(lo, hi uint64) ([]LogEntry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	first, last := fs.boundsLocked()
	if lo < first {
		lo = first
	}
	if hi > last+1 {
		hi = last + 1
	}
	if lo >= hi || len(fs.entries) == 0 {
		return nil, nil
	}

	out := make([]LogEntry, hi-lo)
	copy(out, fs.entries[lo-first:hi-first])
	return out, nil
}

// EntryAt returns the single entry at index.
func (fs *FileStorage) EntryAt(index uint64) (LogEntry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	first, last := fs.boundsLocked()
	if index < first || index > last {
		return LogEntry{}, fmt.Errorf("raft: index %d out of range [%d,%d]", index, first, last)
	}
	return fs.entries[index-first], nil
}

// TermAt returns the term of the entry at index. Index 0 is the sentinel
// "before the log began" and always returns (0, nil).
func (fs *FileStorage) TermAt(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	entry, err := fs.EntryAt(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// FirstIndex returns the index of the oldest entry still in the log.
// TODO(Phase 5): once log compaction lands, this becomes
// snapshotIndex+1 instead of an unconditional 1.
func (fs *FileStorage) FirstIndex() uint64 { return 1 }

// LastIndex returns the index of the newest entry in the log, or 0 if the
// log is empty.
func (fs *FileStorage) LastIndex() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, last := fs.boundsLocked()
	return last
}

// LastTerm returns the term of the newest entry in the log, or 0 if the log
// is empty.
func (fs *FileStorage) LastTerm() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.entries) == 0 {
		return 0
	}
	return fs.entries[len(fs.entries)-1].Term
}

// boundsLocked returns (FirstIndex, LastIndex) for the current in-memory
// tail. Caller must hold fs.mu. LastIndex is FirstIndex-1 when the log is
// empty, so callers can compare bounds without special-casing length 0.
func (fs *FileStorage) boundsLocked() (first, last uint64) {
	first = 1
	if len(fs.entries) == 0 {
		return first, first - 1
	}
	return first, first + uint64(len(fs.entries)) - 1
}

// TruncateAfter discards all entries with index > index by rewriting the
// log file from the surviving prefix (write-tmp, fsync, rename — the same
// atomic-replace pattern SaveHardState uses), then updates the in-memory
// tail. There is no cheap in-place truncation available without tracking
// per-entry byte offsets, and this project's logs are small enough that a
// full rewrite is fine for now; Phase 5's compaction will revisit this.
func (fs *FileStorage) TruncateAfter(index uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	first, last := fs.boundsLocked()
	if index >= last {
		return nil
	}
	keep := fs.entries
	if index < first-1 {
		keep = nil
	} else {
		keep = fs.entries[:index-first+1]
	}

	tmpPath := fs.logPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raft: open tmp log %s: %w", tmpPath, err)
	}

	for _, entry := range keep {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
			f.Close()
			return fmt.Errorf("raft: encode log entry %d: %w", entry.Index, err)
		}
		if err := binary.Write(f, binary.BigEndian, uint32(buf.Len())); err != nil {
			f.Close()
			return fmt.Errorf("raft: write log entry length: %w", err)
		}
		if _, err := f.Write(buf.Bytes()); err != nil {
			f.Close()
			return fmt.Errorf("raft: write log entry %d: %w", entry.Index, err)
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsync tmp log %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close tmp log %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, fs.logPath); err != nil {
		return fmt.Errorf("raft: rename log into place: %w", err)
	}

	fs.entries = keep
	return nil
}

// InitialHardState is a plain accessor used at node startup to seed
// in-memory Raft state from whatever was loaded off disk. It is not part
// of the Storage interface.
func (fs *FileStorage) InitialHardState() (term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.term, fs.votedFor, fs.snapshotIndex, fs.snapshotTerm
}
