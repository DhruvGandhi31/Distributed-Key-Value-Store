package store

import (
	"bytes"
	"testing"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

func applyCmd(t *testing.T, kv *KVStore, cmd Command) *ApplyResult {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	result := kv.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: data})
	res, ok := result.(*ApplyResult)
	if !ok || res == nil {
		t.Fatalf("Apply returned %#v, want *ApplyResult", result)
	}
	return res
}

func TestApply_PutGetDelete(t *testing.T) {
	kv := New()

	res := applyCmd(t, kv, Command{Op: OpPut, Key: "a", Value: []byte("1")})
	if !res.OK || res.Previous != nil {
		t.Fatalf("first Put: got %+v, want OK with no previous value", res)
	}
	val, ok := kv.Get("a")
	if !ok || string(val) != "1" {
		t.Fatalf("Get(a) = (%q, %v), want (1, true)", val, ok)
	}

	res = applyCmd(t, kv, Command{Op: OpPut, Key: "a", Value: []byte("2")})
	if !res.OK || string(res.Previous) != "1" {
		t.Fatalf("overwriting Put: got %+v, want OK with previous=1", res)
	}

	res = applyCmd(t, kv, Command{Op: OpDelete, Key: "a"})
	if !res.OK || string(res.Previous) != "2" {
		t.Fatalf("Delete: got %+v, want OK with previous=2", res)
	}
	if _, ok := kv.Get("a"); ok {
		t.Fatalf("Get(a) after Delete: found a value, want not found")
	}

	res = applyCmd(t, kv, Command{Op: OpDelete, Key: "a"})
	if res.OK {
		t.Fatalf("Delete of an already-missing key: got OK=true, want false")
	}
}

func TestApply_CAS(t *testing.T) {
	kv := New()
	applyCmd(t, kv, Command{Op: OpPut, Key: "k", Value: []byte("old")})

	// wrong Expected value: rejected, no change.
	res := applyCmd(t, kv, Command{Op: OpCAS, Key: "k", Expected: []byte("not-old"), Value: []byte("new")})
	if res.OK {
		t.Fatalf("CAS with a mismatched Expected: got OK=true, want false")
	}
	if val, _ := kv.Get("k"); string(val) != "old" {
		t.Fatalf("value changed despite a rejected CAS: got %q, want old", val)
	}

	// correct Expected value: applied.
	res = applyCmd(t, kv, Command{Op: OpCAS, Key: "k", Expected: []byte("old"), Value: []byte("new")})
	if !res.OK {
		t.Fatalf("CAS with a matching Expected: got OK=false, want true")
	}
	if val, _ := kv.Get("k"); string(val) != "new" {
		t.Fatalf("Get after a successful CAS: got %q, want new", val)
	}

	// CAS against a key that doesn't exist yet: Expected must be nil/empty to succeed.
	res = applyCmd(t, kv, Command{Op: OpCAS, Key: "fresh", Expected: nil, Value: []byte("v")})
	if !res.OK {
		t.Fatalf("CAS on a missing key with Expected=nil: got OK=false, want true")
	}
}

func TestScan_PrefixSortedByKey(t *testing.T) {
	kv := New()
	for _, kvp := range [][2]string{{"b", "2"}, {"a", "1"}, {"ab", "3"}, {"z", "9"}} {
		applyCmd(t, kv, Command{Op: OpPut, Key: kvp[0], Value: []byte(kvp[1])})
	}

	results := kv.Scan("a")
	if len(results) != 2 {
		t.Fatalf("Scan(a) returned %d results, want 2 (a, ab)", len(results))
	}
	if results[0][0] != "a" || results[1][0] != "ab" {
		t.Fatalf("Scan(a) = %v, want sorted [a, ab]", results)
	}

	all := kv.Scan("")
	if len(all) != 4 {
		t.Fatalf("Scan('') returned %d results, want 4", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1][0] > all[i][0] {
			t.Fatalf("Scan('') not sorted: %v", all)
		}
	}
}

func TestSnapshot_RoundTrip(t *testing.T) {
	kv := New()
	applyCmd(t, kv, Command{Op: OpPut, Key: "a", Value: []byte("1")})
	applyCmd(t, kv, Command{Op: OpPut, Key: "b", Value: []byte("2")})

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatalf("Snapshot returned no data for a non-empty store")
	}

	restored := New()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Len() != kv.Len() {
		t.Fatalf("restored.Len() = %d, want %d", restored.Len(), kv.Len())
	}
	if val, ok := restored.Get("a"); !ok || string(val) != "1" {
		t.Fatalf("restored Get(a) = (%q, %v), want (1, true)", val, ok)
	}
	if val, ok := restored.Get("b"); !ok || string(val) != "2" {
		t.Fatalf("restored Get(b) = (%q, %v), want (2, true)", val, ok)
	}
}

func TestRestore_ReplacesRatherThanMerges(t *testing.T) {
	restored := New()
	applyCmd(t, restored, Command{Op: OpPut, Key: "stale", Value: []byte("gone-after-restore")})

	source := New()
	applyCmd(t, source, Command{Op: OpPut, Key: "fresh", Value: []byte("v")})
	snap, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := restored.Get("stale"); ok {
		t.Fatalf("Restore should replace the whole map, but the pre-existing key survived")
	}
	if val, ok := restored.Get("fresh"); !ok || string(val) != "v" {
		t.Fatalf("restored Get(fresh) = (%q, %v), want (v, true)", val, ok)
	}
}

func TestCommand_EncodeDecodeRoundTrip(t *testing.T) {
	c := Command{Op: OpCAS, Key: "k", Value: []byte("v"), Expected: []byte("e")}
	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Op != c.Op || got.Key != c.Key || !bytes.Equal(got.Value, c.Value) || !bytes.Equal(got.Expected, c.Expected) {
		t.Fatalf("round-tripped command = %+v, want %+v", got, c)
	}
}

func TestApply_IgnoresNoOpAndEmptyEntries(t *testing.T) {
	kv := New()
	if result := kv.Apply(raft.LogEntry{Type: raft.EntryNoOp}); result != nil {
		t.Fatalf("Apply on a no-op entry returned %#v, want nil", result)
	}
	if result := kv.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: nil}); result != nil {
		t.Fatalf("Apply on an empty-data normal entry returned %#v, want nil", result)
	}
	if kv.Len() != 0 {
		t.Fatalf("store mutated by an entry that should have been ignored, Len()=%d", kv.Len())
	}
}
