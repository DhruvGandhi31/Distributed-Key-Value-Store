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
	snapshotFileName  = "snapshot"
)

// FileStorage is a Storage implementation backed by three files per node: a
// hard-state file (written atomically: write-tmp, fsync, rename), an
// append-only log file (each entry length-prefixed and gob-encoded, fsync'd
// after every append), and a snapshot file (written atomically, same
// pattern as hard state). A crash mid-write can never leave a
// partially-applied hard-state or snapshot change behind, and can at worst
// leave one torn trailing log entry, which NewFileStorage discards on
// recovery.
//
// The full log tail is also kept in memory, since this project's logs
// (post-compaction) are small. entries[i] is the log entry at index
// firstIndex+i — firstIndex starts at 1 and advances to
// lastIncludedIndex+1 every time a snapshot compacts the log.
type FileStorage struct {
	mu sync.Mutex

	dir          string
	logPath      string
	snapshotPath string

	term     uint64
	votedFor NodeID

	// snapshotIndex/snapshotTerm are the authoritative in-memory record of
	// the most recent snapshot's boundary, populated from the snapshot
	// file itself (not from hardstate's own snapshotIndex/snapshotTerm
	// fields — see SaveHardState's doc comment for why those go unused).
	snapshotIndex uint64
	snapshotTerm  uint64

	firstIndex uint64
	entries    []LogEntry
}

// NewFileStorage opens (or creates) the on-disk state for node id under
// dataDir/id. If no hard-state, log, or snapshot file exists yet, it
// starts fresh with zero-valued state and an empty log.
func NewFileStorage(dataDir string, id NodeID) (*FileStorage, error) {
	dir := filepath.Join(dataDir, string(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("raft: create data dir %s: %w", dir, err)
	}

	fs := &FileStorage{
		dir:          dir,
		logPath:      filepath.Join(dir, logFileName),
		snapshotPath: filepath.Join(dir, snapshotFileName),
		firstIndex:   1,
	}

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

	// Load the snapshot boundary (if any) BEFORE the log, so loadLog knows
	// which entries are already covered by the snapshot and safe to skip.
	// This overrides whatever decodeHardState set for snapshotIndex/Term —
	// the snapshot file is authoritative, hardstate's copy is unused.
	_, snapIndex, snapTerm, err := fs.readSnapshotFile()
	if err != nil {
		return nil, fmt.Errorf("raft: read snapshot %s: %w", fs.snapshotPath, err)
	}
	fs.snapshotIndex = snapIndex
	fs.snapshotTerm = snapTerm
	fs.firstIndex = snapIndex + 1

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

	// snapshotIndex/snapshotTerm are still read here to stay consistent
	// with the on-disk format, but discarded: readSnapshotFile (called
	// right after, in NewFileStorage) is the authoritative source and
	// overwrites fs.snapshotIndex/fs.snapshotTerm regardless.
	var snapshotIndex, snapshotTerm uint64
	if err := binary.Read(br, binary.BigEndian, &snapshotIndex); err != nil {
		return err
	}
	if err := binary.Read(br, binary.BigEndian, &snapshotTerm); err != nil {
		return err
	}

	fs.term = term
	fs.votedFor = NodeID(votedForBytes)
	return nil
}

// readSnapshotFile reads the snapshot file in full: [lastIndex
// uint64][lastTerm uint64][dataLen uint64][data]. Returns (nil, 0, 0, nil)
// if no snapshot file exists yet — that's a valid "none" result, not an
// error.
func (fs *FileStorage) readSnapshotFile() (data []byte, lastIndex, lastTerm uint64, err error) {
	f, err := os.Open(fs.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	if err := binary.Read(br, binary.BigEndian, &lastIndex); err != nil {
		return nil, 0, 0, err
	}
	if err := binary.Read(br, binary.BigEndian, &lastTerm); err != nil {
		return nil, 0, 0, err
	}
	var dataLen uint64
	if err := binary.Read(br, binary.BigEndian, &dataLen); err != nil {
		return nil, 0, 0, err
	}
	data = make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(br, data); err != nil {
			return nil, 0, 0, err
		}
	}
	return data, lastIndex, lastTerm, nil
}

// loadLog scans the log file from the beginning, decoding each
// length-prefixed gob entry and keeping only those with index >
// snapshotIndex (anything at or before that is already covered by the
// snapshot). A truncated trailing record (a crash mid-write left a length
// prefix with fewer than `len` bytes following it, or fewer than 4 bytes
// for the prefix itself) is treated as the end of the log, not an error —
// everything durably written before it is still valid.
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
		if entry.Index <= fs.snapshotIndex {
			continue // already covered by the snapshot
		}
		entries = append(entries, entry)
	}

	fs.entries = entries
	return nil
}

// SaveHardState persists term/votedFor durably before returning. The write
// is fsync'd before the temp file is renamed into place, and the rename
// itself is atomic — a crash can never observe a partially-written hard
// state file.
//
// The snapshotIndex/snapshotTerm parameters are written to disk (to keep
// the on-disk format stable) but are NOT authoritative for recovery — the
// snapshot file itself is (see readSnapshotFile). Every existing call site
// in election.go passes 0 for these, which would corrupt a real snapshot
// boundary if this file's copy were ever trusted; making the snapshot file
// the single source of truth sidesteps that instead of requiring every
// term/vote-only call site to also track and re-pass the current snapshot
// boundary.
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
	return nil
}

// AppendEntries durably appends entries to the log file, fsyncing once
// after writing all of them, then updates the in-memory tail. The log file
// is opened fresh for each call (rather than holding a long-lived handle)
// so that TruncateAfter/SaveSnapshot's rewrite-and-rename is always
// followed by a clean re-open, with no stale file handle left pointing at
// the old inode.
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

	first := fs.firstIndex
	last := fs.lastIndexLocked()
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

	first := fs.firstIndex
	last := fs.lastIndexLocked()
	if index < first || index > last {
		return LogEntry{}, fmt.Errorf("raft: index %d out of range [%d,%d]", index, first, last)
	}
	return fs.entries[index-first], nil
}

// TermAt returns the term of the entry at index. Index 0 is the sentinel
// "before the log began" and always returns (0, nil). The most recent
// snapshot's lastIncludedIndex also succeeds even though that entry itself
// is no longer in the log — see the Storage interface doc comment for why.
func (fs *FileStorage) TermAt(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	fs.mu.Lock()
	if index == fs.snapshotIndex {
		term := fs.snapshotTerm
		fs.mu.Unlock()
		return term, nil
	}
	fs.mu.Unlock()

	entry, err := fs.EntryAt(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// FirstIndex returns the index of the oldest entry still in the log
// (lastIncludedIndex+1 after a snapshot, 1 if none has been taken yet).
func (fs *FileStorage) FirstIndex() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.firstIndex
}

// LastIndex returns the index of the newest entry in the log, falling back
// to the snapshot's lastIncludedIndex if the log is empty because
// everything has been compacted, or 0 if neither exists yet.
func (fs *FileStorage) LastIndex() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.lastIndexLocked()
}

// LastTerm returns the term of the newest entry in the log, with the same
// snapshot fallback as LastIndex.
func (fs *FileStorage) LastTerm() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.entries) == 0 {
		return fs.snapshotTerm
	}
	return fs.entries[len(fs.entries)-1].Term
}

// lastIndexLocked returns LastIndex's value. Caller must hold fs.mu.
func (fs *FileStorage) lastIndexLocked() uint64 {
	if len(fs.entries) == 0 {
		return fs.snapshotIndex
	}
	return fs.firstIndex + uint64(len(fs.entries)) - 1
}

// TruncateAfter discards all entries with index > index by rewriting the
// log file from the surviving prefix (write-tmp, fsync, rename — the same
// atomic-replace pattern SaveHardState uses), then updates the in-memory
// tail. There is no cheap in-place truncation available without tracking
// per-entry byte offsets, and this project's logs are small enough
// (post-compaction) that a full rewrite is fine.
func (fs *FileStorage) TruncateAfter(index uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	first := fs.firstIndex
	last := fs.lastIndexLocked()
	if index >= last {
		return nil
	}
	var keep []LogEntry
	if index >= first-1 && len(fs.entries) > 0 {
		keep = fs.entries[:index-first+1]
	}

	if err := fs.rewriteLogLocked(keep); err != nil {
		return err
	}

	fs.entries = keep
	return nil
}

// rewriteLogLocked overwrites the log file with exactly keep, atomically
// (write-tmp, fsync, rename). Caller must hold fs.mu.
func (fs *FileStorage) rewriteLogLocked(keep []LogEntry) error {
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
	return nil
}

// SaveSnapshot persists data as a snapshot atomically (write-tmp, fsync,
// rename — same pattern as SaveHardState), then compacts the log by
// discarding entries with index <= lastIndex and rewriting the log file
// with whatever's left. A lastIndex at or before the current snapshot
// boundary is a no-op (defends against a redundant/stale snapshot call).
func (fs *FileStorage) SaveSnapshot(data []byte, lastIndex, lastTerm uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if lastIndex <= fs.snapshotIndex {
		return nil
	}

	tmpPath := fs.snapshotPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raft: open tmp snapshot %s: %w", tmpPath, err)
	}
	if err := binary.Write(f, binary.BigEndian, lastIndex); err != nil {
		f.Close()
		return fmt.Errorf("raft: write snapshot lastIndex: %w", err)
	}
	if err := binary.Write(f, binary.BigEndian, lastTerm); err != nil {
		f.Close()
		return fmt.Errorf("raft: write snapshot lastTerm: %w", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint64(len(data))); err != nil {
		f.Close()
		return fmt.Errorf("raft: write snapshot data length: %w", err)
	}
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			f.Close()
			return fmt.Errorf("raft: write snapshot data: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsync snapshot %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close snapshot %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, fs.snapshotPath); err != nil {
		return fmt.Errorf("raft: rename snapshot into place: %w", err)
	}

	// Compact: keep only entries strictly after lastIndex. lastIndex is
	// always >= fs.firstIndex here, since the early return above already
	// ruled out lastIndex <= fs.snapshotIndex (== fs.firstIndex-1).
	var keep []LogEntry
	if lastIndex < fs.lastIndexLocked() {
		keep = fs.entries[lastIndex-fs.firstIndex+1:]
	}
	if err := fs.rewriteLogLocked(keep); err != nil {
		return err
	}

	fs.entries = keep
	fs.firstIndex = lastIndex + 1
	fs.snapshotIndex = lastIndex
	fs.snapshotTerm = lastTerm
	return nil
}

// LoadSnapshot returns the most recently saved snapshot, or (nil, 0, 0,
// nil) if none has been taken yet.
func (fs *FileStorage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	return fs.readSnapshotFile()
}

// InitialHardState is a plain accessor used at node startup to seed
// in-memory Raft state from whatever was loaded off disk. It is not part
// of the Storage interface. snapshotIndex/snapshotTerm here reflect the
// same authoritative (snapshot-file-derived) values LoadSnapshot returns,
// not hardstate's unused copy.
func (fs *FileStorage) InitialHardState() (term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.term, fs.votedFor, fs.snapshotIndex, fs.snapshotTerm
}
