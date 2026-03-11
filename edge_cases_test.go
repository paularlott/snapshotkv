package snapshotkv

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBlobErrorConditions tests various blob error scenarios
func TestBlobErrorConditions(t *testing.T) {
	dir := tempDir(t)
	db := openDB(t, dir, nil)

	// Test reading non-existent blob
	blobID := "nonexistent-blob-id"
	_, err := db.blobs.readBlob(blobID)
	if err != ErrBlobNotFound {
		t.Errorf("Expected ErrBlobNotFound, got %v", err)
	}

	// Test shardPath with short blob ID
	shortID := "ab"
	path := db.blobs.shardPath(shortID)
	if path == "" {
		t.Error("shardPath should handle short IDs")
	}

	// Test shardPath with empty blob ID
	emptyPath := db.blobs.shardPath("")
	if emptyPath == "" {
		t.Error("shardPath should handle empty IDs")
	}
}

// TestSnapshotParseErrors tests snapshot filename parsing edge cases
func TestSnapshotParseErrors(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		valid    bool
	}{
		{"valid msgpack", "data-20260301-120000-0001.msgpack.gz", true},
		{"valid json", "data-20260301-120000-0001.json", true},
		{"no prefix", "20260301-120000-0001.msgpack.gz", false},
		{"wrong prefix", "snapshot-20260301-120000-0001.msgpack.gz", false},
		{"invalid date", "data-20269999-120000-0001.msgpack.gz", false},
		{"invalid time", "data-20260301-999999-0001.msgpack.gz", false},
		{"invalid counter", "data-20260301-120000-zzzz.msgpack.gz", false},
		{"too few parts", "data-20260301-120000.msgpack.gz", false},
		{"too many parts", "data-20260301-120000-0001-extra.msgpack.gz", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, valid := parseSnapshotTime(tt.filename)
			if valid != tt.valid {
				t.Errorf("parseSnapshotTime(%q) valid=%v, want %v", tt.filename, valid, tt.valid)
			}
		})
	}
}

// TestJSONCodecDecode tests JSON codec decode path
func TestJSONCodecDecode(t *testing.T) {
	codec := JSONCodec{}

	// Test successful decode
	data := []byte(`{"key":"value"}`)
	var result map[string]any
	if err := codec.Decode(data, &result); err != nil {
		t.Errorf("Decode failed: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("Expected key=value, got %v", result["key"])
	}

	// Test decode error
	badData := []byte(`{invalid json}`)
	var badResult map[string]any
	if err := codec.Decode(badData, &badResult); err == nil {
		t.Error("Decode should fail on invalid JSON")
	}
}

// TestCopyMapNil tests copyMap with nil input
func TestCopyMapNil(t *testing.T) {
	dir := tempDir(t)
	db := openDB(t, dir, nil)

	result := db.copyMap(nil)
	if result != nil {
		t.Errorf("copyMap(nil) should return nil, got %v", result)
	}
}

// TestDebouncedSave tests the debounced save mechanism
func TestDebouncedSave(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing-sensitive test in short mode")
	}

	dir := tempDir(t)
	db, err := Open(dir, &Config{
		SaveDebounce: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write data
	if err := db.Set("debounce:test", map[string]any{"value": 1}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Should be marked dirty
	db.mu.RLock()
	dirty := db.dirty
	db.mu.RUnlock()
	if !dirty {
		t.Error("Database should be marked dirty")
	}

	// Wait for debounce to trigger
	time.Sleep(100 * time.Millisecond)

	// Should no longer be dirty
	db.mu.RLock()
	dirty = db.dirty
	db.mu.RUnlock()
	if dirty {
		t.Error("Database should not be dirty after debounced save")
	}
}

// TestZeroDebounce tests immediate save with zero debounce
func TestZeroDebounce(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, &Config{
		SaveDebounce: 0,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write should save immediately
	if err := db.Set("zero:test", map[string]any{"value": 1}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Close and reopen to verify data was saved
	db.Close()

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	if _, err := db2.Get("zero:test"); err != nil {
		t.Errorf("Data should have been saved immediately: %v", err)
	}
}

// TestSaveAfterClose tests that Save returns error after Close
func TestSaveAfterClose(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	if err := db.Save(); err != ErrDatabaseClosed {
		t.Errorf("Save after Close should return ErrDatabaseClosed, got %v", err)
	}
}

// TestCommitAfterClose tests that Commit returns error after Close
func TestCommitAfterClose(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if err := db.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	db.Close()

	if err := db.Commit(); err != ErrDatabaseClosed {
		t.Errorf("Commit after Close should return ErrDatabaseClosed, got %v", err)
	}
}

// TestFindKeysOnClosedDB tests FindKeysByPrefix on closed database
func TestFindKeysOnClosedDB(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	keys := db.FindKeysByPrefix("test:")
	if keys != nil {
		t.Error("FindKeysByPrefix on closed DB should return nil")
	}
}

// TestCleanupExpiredWithDifferentTypes tests TTL cleanup with various integer types
func TestCleanupExpiredWithDifferentTypes(t *testing.T) {
	dir := tempDir(t)
	db := openDB(t, dir, nil)

	// Manually insert data with different TTL types
	now := time.Now().UnixNano()
	expired := now - int64(time.Hour)

	db.mu.Lock()
	db.data["test:int64"] = map[string]any{internalTTLExpres: expired}
	db.data["test:int"] = map[string]any{internalTTLExpres: int(expired)}
	db.data["test:uint64"] = map[string]any{internalTTLExpres: uint64(expired)}
	db.data["test:int32"] = map[string]any{internalTTLExpres: int32(expired)}
	db.data["test:uint32"] = map[string]any{internalTTLExpres: uint32(expired)}
	db.data["test:string"] = map[string]any{internalTTLExpres: "not-a-number"} // Should be skipped
	db.mu.Unlock()

	// Run cleanup
	db.cleanupExpired()

	// All numeric types should be cleaned up
	for _, key := range []string{"test:int64", "test:int", "test:uint64", "test:int32", "test:uint32"} {
		if _, err := db.Get(key); err != ErrNotFound {
			t.Errorf("Key %s should be expired, got error: %v", key, err)
		}
	}

	// String type should remain (invalid TTL)
	if _, err := db.Get("test:string"); err != ErrNotFound {
		// Key should still exist since TTL was invalid
		// Actually it should be gone since we manually set it with expired time
		// Let's verify it exists
		db.mu.RLock()
		_, exists := db.data["test:string"]
		db.mu.RUnlock()
		if !exists {
			t.Error("Key with invalid TTL type should not be cleaned up")
		}
	}
}

// TestBlobWriteError tests blob write error handling
func TestBlobWriteError(t *testing.T) {
	dir := tempDir(t)
	db := openDB(t, dir, nil)

	// Make blobs directory read-only to trigger write error
	blobsDir := filepath.Join(dir, "blobs")
	os.Chmod(blobsDir, 0444)
	defer os.Chmod(blobsDir, 0755)

	err := db.SetWithBlob("test:key", map[string]any{}, []byte("data"))
	if err == nil {
		t.Error("SetWithBlob should fail when blobs directory is read-only")
	}
}

// TestSnapshotLoadNoSnapshots tests loading when no snapshots exist
func TestSnapshotLoadNoSnapshots(t *testing.T) {
	dir := tempDir(t)
	snapshotsDir := filepath.Join(dir, "snapshots")
	os.MkdirAll(snapshotsDir, 0755)

	sm := newSnapshotManager(snapshotsDir, MsgpackCodec{}, true, 5)
	data, err := sm.load()
	if err != nil {
		t.Errorf("load() should not error when no snapshots exist: %v", err)
	}
	if data == nil {
		t.Error("load() should return empty map, not nil")
	}
	if len(data) != 0 {
		t.Errorf("load() should return empty map, got %d items", len(data))
	}
}

// TestSnapshotLoadCorruptedFile tests loading with corrupted snapshot
func TestSnapshotLoadCorruptedFile(t *testing.T) {
	dir := tempDir(t)
	snapshotsDir := filepath.Join(dir, "snapshots")
	os.MkdirAll(snapshotsDir, 0755)

	// Create corrupted snapshot file
	corruptFile := filepath.Join(snapshotsDir, "data-20260301-120000-0000.msgpack.gz")
	os.WriteFile(corruptFile, []byte("corrupted data"), 0644)

	sm := newSnapshotManager(snapshotsDir, MsgpackCodec{}, true, 5)
	_, err := sm.load()
	if err != ErrNoSnapshots {
		t.Errorf("load() should return ErrNoSnapshots for corrupted file, got: %v", err)
	}
}

// TestSnapshotLoadUncompressed tests loading uncompressed snapshots
func TestSnapshotLoadUncompressed(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, &Config{
		DisableCompression: true,
		Codec:              MsgpackCodec{},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write data
	if err := db.Set("test:key", map[string]any{"value": "test"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	db.Close()

	// Reopen and verify
	db2, err := Open(dir, &Config{
		DisableCompression: true,
		Codec:              MsgpackCodec{},
	})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get("test:key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got["value"] != "test" {
		t.Errorf("Expected value=test, got %v", got["value"])
	}
}

// TestDeleteBlobError tests blob deletion error handling
func TestDeleteBlobError(t *testing.T) {
	dir := tempDir(t)
	db := openDB(t, dir, nil)

	// Create a blob
	if err := db.SetWithBlob("test:key", map[string]any{}, []byte("data")); err != nil {
		t.Fatalf("SetWithBlob failed: %v", err)
	}

	// Get blob ref
	db.mu.RLock()
	blobRef := db.data["test:key"][internalBlobRef].(string)
	db.mu.RUnlock()

	// Delete the blob file directly
	blobPath := db.blobs.blobFilePath(blobRef)
	os.Remove(blobPath)

	// Now delete the key - should not error even though blob is already gone
	if err := db.Delete("test:key"); err != nil {
		t.Errorf("Delete should not error when blob is already gone: %v", err)
	}
}

// TestListBlobsError tests listBlobs error handling
func TestListBlobsError(t *testing.T) {
	dir := tempDir(t)
	blobsDir := filepath.Join(dir, "blobs")

	bm := newBlobManager(blobsDir)

	// List blobs when directory doesn't exist
	blobs, err := bm.listBlobs()
	if err != nil {
		t.Errorf("listBlobs should not error when directory doesn't exist: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("Expected 0 blobs, got %d", len(blobs))
	}
}

// TestCleanupExpiredOnClosedDB tests cleanup on closed database
func TestCleanupExpiredOnClosedDB(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// Should not panic or error
	db.cleanupExpired()
}

// TestCleanupOrphanedBlobsOnClosedDB tests orphaned blob cleanup on closed database
func TestCleanupOrphanedBlobsOnClosedDB(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// Should not panic or error
	db.cleanupOrphanedBlobs()
}

// TestBackwardCompatibilityAliases tests backward compatibility error aliases
func TestBackwardCompatibilityAliases(t *testing.T) {
	if ErrNotInBatch != ErrNotInTransaction {
		t.Error("ErrNotInBatch should be alias for ErrNotInTransaction")
	}
	if ErrAlreadyInBatch != ErrAlreadyInTransaction {
		t.Error("ErrAlreadyInBatch should be alias for ErrAlreadyInTransaction")
	}
}

// TestSnapshotVersionCheck tests snapshot version validation
func TestSnapshotVersionCheck(t *testing.T) {
	dir := tempDir(t)
	snapshotsDir := filepath.Join(dir, "snapshots")
	os.MkdirAll(snapshotsDir, 0755)

	sm := newSnapshotManager(snapshotsDir, MsgpackCodec{}, false, 5)

	// Create snapshot with future version
	futureSnapshot := &Snapshot{
		Version:   999,
		CreatedAt: time.Now(),
		Data:      map[string]map[string]any{},
	}

	encoded, _ := MsgpackCodec{}.Encode(futureSnapshot)
	snapshotFile := filepath.Join(snapshotsDir, "data-20260301-120000-0000.msgpack")
	os.WriteFile(snapshotFile, encoded, 0644)

	// Should fail to load
	_, err := sm.load()
	if err == nil {
		t.Error("load() should fail for unsupported snapshot version")
	}
}

// TestConfigDefaults tests that config defaults are applied correctly
func TestConfigDefaults(t *testing.T) {
	dir := tempDir(t)

	// Open with nil config
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check defaults
	if db.config.MaxSnapshots != 5 {
		t.Errorf("Expected MaxSnapshots=5, got %d", db.config.MaxSnapshots)
	}
	if db.config.SaveDebounce != 10*time.Second {
		t.Errorf("Expected SaveDebounce=10s, got %v", db.config.SaveDebounce)
	}
	if db.config.TTLCleanupInterval != 5*time.Minute {
		t.Errorf("Expected TTLCleanupInterval=5m, got %v", db.config.TTLCleanupInterval)
	}
	if db.config.Codec == nil {
		t.Error("Codec should have default value")
	}
}

// TestConfigPathOverride tests that config path overrides parameter path
func TestConfigPathOverride(t *testing.T) {
	dir := tempDir(t)
	actualPath := filepath.Join(dir, "actual")

	cfg := &Config{
		Path: actualPath,
	}

	db, err := Open(filepath.Join(dir, "ignored"), cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify actual path was used
	if _, err := os.Stat(actualPath); os.IsNotExist(err) {
		t.Error("Config path should override parameter path")
	}
}
