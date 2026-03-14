package snapshotkv

import (
	"testing"
	"time"
)

// TestMemoryOnlyMode tests database in memory-only mode
func TestMemoryOnlyMode(t *testing.T) {
	db, err := Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open memory-only database: %v", err)
	}
	defer db.Close()

	if !db.IsMemoryOnly() {
		t.Error("Database should be in memory-only mode")
	}

	// Basic operations should work
	key := "mem:test"
	value := map[string]any{"name": "test"}

	if err := db.Set(key, value); err != nil {
		t.Fatalf("Set should work in memory-only mode: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get should work in memory-only mode: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("Expected map[string]any, got %T", got)
	}
	if m["name"] != "test" {
		t.Errorf("Expected name=test, got %v", m["name"])
	}

	// Save should be no-op
	if err := db.Save(); err != nil {
		t.Errorf("Save should not error in memory-only mode: %v", err)
	}
}

// TestMemoryOnlyBlobOperations tests that blob operations fail in memory-only mode
func TestMemoryOnlyBlobOperations(t *testing.T) {
	db, err := Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open memory-only database: %v", err)
	}
	defer db.Close()

	key := "mem:blob"
	value := map[string]any{"name": "test"}
	blob := []byte("data")

	// SetWithBlob should fail
	err = db.SetWithBlob(key, value, blob)
	if err != ErrMemoryOnly {
		t.Errorf("SetWithBlob should return ErrMemoryOnly, got %v", err)
	}

	// SetWithBlobEx should fail
	err = db.SetWithBlobEx(key, value, blob, time.Hour)
	if err != ErrMemoryOnly {
		t.Errorf("SetWithBlobEx should return ErrMemoryOnly, got %v", err)
	}

	// Set regular key
	if err := db.Set(key, value); err != nil {
		t.Fatalf("Set should work: %v", err)
	}

	// GetBlob should fail
	_, err = db.GetBlob(key)
	if err != ErrMemoryOnly {
		t.Errorf("GetBlob should return ErrMemoryOnly, got %v", err)
	}

	// UpdateBlob should fail
	err = db.UpdateBlob(key, blob)
	if err != ErrMemoryOnly {
		t.Errorf("UpdateBlob should return ErrMemoryOnly, got %v", err)
	}
}

// TestMemoryOnlyTTL tests TTL cleanup in memory-only mode
func TestMemoryOnlyTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TTL test in short mode")
	}

	db, err := Open("", &Config{
		TTLCleanupInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := "mem:ttl"
	if err := db.SetEx(key, map[string]any{"test": "value"}, 50*time.Millisecond); err != nil {
		t.Fatalf("SetEx failed: %v", err)
	}

	// Should exist
	if _, err := db.Get(key); err != nil {
		t.Fatalf("Key should exist: %v", err)
	}

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)
	db.cleanupExpired()

	// Should be gone
	if _, err := db.Get(key); err != ErrNotFound {
		t.Errorf("Key should be expired: %v", err)
	}
}

// TestMemoryOnlyTransaction tests transactions in memory-only mode
func TestMemoryOnlyTransaction(t *testing.T) {
	db, err := Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := db.Set("key", map[string]any{"i": i}); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	if err := db.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data exists
	if _, err := db.Get("key"); err != nil {
		t.Errorf("Key should exist after commit: %v", err)
	}
}

// TestMemoryOnlyDisableTTLCleanup tests disabling TTL cleanup
func TestMemoryOnlyDisableTTLCleanup(t *testing.T) {
	db, err := Open("", &Config{
		TTLCleanupInterval: 0, // Disable
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Should work fine
	if err := db.Set("key", map[string]any{"test": "value"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}
