package snapshotkv

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// Config holds database configuration
type Config struct {
	// Path is the base directory for storage (empty = memory-only mode)
	Path string
	// MaxSnapshots is the maximum number of snapshot files to keep (default: 5)
	MaxSnapshots int
	// SaveDebounce is the debounce interval for saves (default: 10s)
	SaveDebounce time.Duration
	// DisableCompression disables gzip compression for snapshots (default: false, compression enabled)
	DisableCompression bool
	// TTLCleanupInterval is the interval for TTL cleanup (default: 5m, 0 to disable)
	TTLCleanupInterval time.Duration
	// Codec is the encoder/decoder (default: MsgpackCodec)
	Codec Codec
}

// entry holds a stored value with its metadata
type entry struct {
	Value     []byte `msgpack:"value" json:"value"`
	ExpiresAt int64  `msgpack:"expires_at" json:"expires_at"` // UnixNano, 0 = no expiration
	BlobRef   string `msgpack:"blob_ref" json:"blob_ref"`     // empty = no blob
}

// isExpired returns true if the entry has expired
func (e *entry) isExpired() bool {
	if e.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixNano() > e.ExpiresAt
}

// DB is the main database structure
type DB struct {
	mu        sync.RWMutex
	data      map[string]*entry
	config    *Config
	snapshots *snapshotManager
	blobs     *blobManager
	closed    bool

	// Memory-only mode flag
	memoryOnly bool

	// Transaction mode
	inTransaction    bool
	transactionMu    sync.Mutex
	transactionBlobs []string // Blobs created during transaction

	// Debounced save
	dirty       bool
	saveTimer   *time.Timer
	saveTimerMu sync.Mutex

	// TTL cleanup
	ttlStopCh chan struct{}
	ttlWg     sync.WaitGroup
}

// Open opens or creates a database at the given path.
// If path is empty, operates in memory-only mode with no persistence or blob support.
func Open(path string, cfg *Config) (*DB, error) {
	// Apply defaults
	if cfg == nil {
		cfg = &Config{}
	}

	cfg.Path = cmp.Or(cfg.Path, path)
	cfg.MaxSnapshots = cmp.Or(cfg.MaxSnapshots, 5)
	cfg.SaveDebounce = cmp.Or(cfg.SaveDebounce, 10*time.Second)
	cfg.TTLCleanupInterval = cmp.Or(cfg.TTLCleanupInterval, 5*time.Minute)
	if cfg.Codec == nil {
		cfg.Codec = MsgpackCodec{}
	}

	// Memory-only mode when path is empty
	memoryOnly := cfg.Path == ""

	db := &DB{
		data:       make(map[string]*entry),
		config:     cfg,
		memoryOnly: memoryOnly,
		ttlStopCh:  make(chan struct{}),
	}

	// Skip all file operations in memory-only mode
	if !memoryOnly {
		// Create directories
		if err := os.MkdirAll(cfg.Path, 0755); err != nil {
			return nil, fmt.Errorf("failed to create database directory: %w", err)
		}

		snapshotsPath := filepath.Join(cfg.Path, "snapshots")
		blobsPath := filepath.Join(cfg.Path, "blobs")

		// Create subdirectories
		if err := os.MkdirAll(snapshotsPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create snapshots directory: %w", err)
		}
		if err := os.MkdirAll(blobsPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create blobs directory: %w", err)
		}

		db.snapshots = newSnapshotManager(snapshotsPath, cfg.Codec, !cfg.DisableCompression, cfg.MaxSnapshots)
		db.blobs = newBlobManager(blobsPath)

		// Clean up temp files from incomplete writes
		db.snapshots.cleanupTempFiles()
		db.blobs.cleanupTempFiles()

		// Load existing snapshot
		data, err := db.snapshots.load()
		if err != nil && err != ErrNoSnapshots {
			return nil, fmt.Errorf("failed to load snapshot: %w", err)
		}
		db.data = data

		// Clean up orphaned blobs in background
		go db.cleanupOrphanedBlobs()
	}

	// Start TTL cleanup goroutine
	if cfg.TTLCleanupInterval > 0 {
		db.ttlWg.Add(1)
		go db.ttlCleanupLoop()
	}

	return db, nil
}

// IsMemoryOnly returns true if the database is in memory-only mode
func (db *DB) IsMemoryOnly() bool {
	return db.memoryOnly
}

// Close closes the database and stops background goroutines
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	// Stop TTL cleanup
	if db.ttlStopCh != nil {
		close(db.ttlStopCh)
	}
	db.ttlWg.Wait()

	// Stop debounce timer and flush immediately (skip in memory-only mode)
	if !db.memoryOnly {
		db.stopTimerAndFlush()
	}

	return nil
}

// Save forces an immediate snapshot save (no-op in memory-only mode)
func (db *DB) Save() error {
	if db.memoryOnly {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	return db.saveLocked()
}

// saveLocked saves a snapshot (must be called with lock held)
func (db *DB) saveLocked() error {
	if db.memoryOnly {
		return nil
	}
	return db.snapshots.save(db.data)
}

// checkClosed returns an error if the database is closed
func (db *DB) checkClosed() error {
	if db.closed {
		return ErrDatabaseClosed
	}
	return nil
}

// markDirty marks the database as dirty and schedules a debounced save
func (db *DB) markDirty() {
	if db.memoryOnly || db.inTransaction {
		return
	}

	db.saveTimerMu.Lock()
	defer db.saveTimerMu.Unlock()

	// Already dirty, timer running
	if db.dirty {
		return
	}

	db.dirty = true

	// If debounce is 0, save immediately
	if db.config.SaveDebounce == 0 {
		db.saveLocked()
		db.dirty = false
		return
	}

	// Start debounce timer
	if db.saveTimer == nil {
		db.saveTimer = time.AfterFunc(db.config.SaveDebounce, db.debouncedSave)
	} else {
		db.saveTimer.Reset(db.config.SaveDebounce)
	}
}

// debouncedSave is called after the debounce timer fires
func (db *DB) debouncedSave() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed || !db.dirty {
		return
	}

	db.saveLocked()
	db.dirty = false
}

// stopTimerAndFlush cancels any pending save and saves immediately if dirty
func (db *DB) stopTimerAndFlush() {
	db.saveTimerMu.Lock()
	if db.saveTimer != nil {
		db.saveTimer.Stop()
	}
	db.saveTimerMu.Unlock()

	if db.dirty {
		db.saveLocked()
		db.dirty = false
	}
}

// Get retrieves a value by key.
// Returns ErrNotFound if the key doesn't exist or has expired.
func (db *DB) Get(key string) (any, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if err := db.checkClosed(); err != nil {
		return nil, err
	}

	e, exists := db.data[key]
	if !exists {
		return nil, ErrNotFound
	}

	if e.isExpired() {
		return nil, ErrNotFound
	}

	var v any
	if err := db.config.Codec.Decode(e.Value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// Set serialises value using the DB codec and stores it without expiry.
func (db *DB) Set(key string, value any) error {
	return db.SetEx(key, value, 0)
}

// SetEx serialises value using the DB codec and stores it with TTL.
// Use ttl=0 for no expiration.
func (db *DB) SetEx(key string, value any, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	return db.setLocked(key, value, ttl)
}

// setLocked serialises and stores a value (must be called with lock held)
func (db *DB) setLocked(key string, value any, ttl time.Duration) error {
	encoded, err := db.config.Codec.Encode(value)
	if err != nil {
		return fmt.Errorf("snapshotkv: encode: %w", err)
	}

	e := &entry{Value: encoded}
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl).UnixNano()
	}

	db.data[key] = e
	db.markDirty()
	return nil
}

// Delete removes a key and its associated blob.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	return db.deleteLocked(key)
}

// deleteLocked removes a key (must be called with lock held)
func (db *DB) deleteLocked(key string) error {
	entry, exists := db.data[key]
	if !exists {
		return nil // Already gone
	}

	// Delete associated blob if exists (and not in memory-only mode)
	if !db.memoryOnly && entry.BlobRef != "" {
		db.blobs.deleteBlob(entry.BlobRef)
	}

	delete(db.data, key)

	// Mark dirty for debounced save
	db.markDirty()

	return nil
}

// Exists checks if a key exists and is not expired.
func (db *DB) Exists(key string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return false
	}

	entry, exists := db.data[key]
	if !exists {
		return false
	}

	return !entry.isExpired()
}

// TTL returns the remaining time-to-live for a key.
// Returns -1 if the key has no expiration, -2 if the key doesn't exist.
func (db *DB) TTL(key string) time.Duration {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return -2 * time.Second
	}

	entry, exists := db.data[key]
	if !exists {
		return -2 * time.Second // Key doesn't exist
	}

	if entry.isExpired() {
		return -2 * time.Second // Key has expired
	}

	if entry.ExpiresAt == 0 {
		return -1 * time.Second // No expiration
	}

	now := time.Now().UnixNano()
	if now >= entry.ExpiresAt {
		return -2 * time.Second // Already expired
	}

	return time.Duration(entry.ExpiresAt - now)
}

// Count returns the number of non-expired keys matching a prefix.
func (db *DB) Count(prefix string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return 0
	}

	count := 0
	for key, e := range db.data {
		if e.isExpired() {
			continue
		}
		if len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
			count++
		}
	}
	return count
}

// FindKeysByPrefix returns all non-expired keys matching a prefix.
func (db *DB) FindKeysByPrefix(prefix string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	var keys []string
	for key, entry := range db.data {
		if entry.isExpired() {
			continue
		}
		if len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
			keys = append(keys, key)
		}
	}

	// Sort keys for consistent ordering
	slices.Sort(keys)
	return keys
}

// Scan iterates over all non-expired keys matching prefix, calling fn for each.
// Stops early if fn returns false. The lock is held for the duration of the scan
// so fn must not call any DB methods.
func (db *DB) Scan(prefix string, fn func(key string, value any) bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return
	}

	for key, e := range db.data {
		if e.isExpired() {
			continue
		}
		if len(prefix) > 0 && (len(key) < len(prefix) || key[:len(prefix)] != prefix) {
			continue
		}
		var v any
		if err := db.config.Codec.Decode(e.Value, &v); err != nil {
			continue
		}
		if !fn(key, v) {
			return
		}
	}
}

// SetWithBlob stores a value with blob data (no expiry).
// Returns ErrMemoryOnly in memory-only mode.
func (db *DB) SetWithBlob(key string, value any, blob []byte) error {
	return db.SetWithBlobEx(key, value, blob, 0)
}

// SetWithBlobEx stores a value with blob data and TTL.
// Returns ErrMemoryOnly in memory-only mode.
func (db *DB) SetWithBlobEx(key string, value any, blob []byte, ttl time.Duration) error {
	if db.memoryOnly {
		return ErrMemoryOnly
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	encoded, err := db.config.Codec.Encode(value)
	if err != nil {
		return fmt.Errorf("snapshotkv: encode: %w", err)
	}

	// Write blob
	blobRef, err := db.blobs.writeBlob(blob)
	if err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}

	// Track blob for transaction rollback
	if db.inTransaction {
		db.transactionBlobs = append(db.transactionBlobs, blobRef)
	}

	e := &entry{
		Value:   encoded,
		BlobRef: blobRef,
	}
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl).UnixNano()
	}

	db.data[key] = e
	db.markDirty()
	return nil
}

// GetBlob retrieves the blob data for a key.
// Returns ErrMemoryOnly in memory-only mode.
// Returns ErrNoBlob if the key has no associated blob.
func (db *DB) GetBlob(key string) ([]byte, error) {
	if db.memoryOnly {
		return nil, ErrMemoryOnly
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if err := db.checkClosed(); err != nil {
		return nil, err
	}

	e, exists := db.data[key]
	if !exists {
		return nil, ErrNotFound
	}

	if e.isExpired() {
		return nil, ErrNotFound
	}

	if e.BlobRef == "" {
		return nil, ErrNoBlob
	}

	return db.blobs.readBlob(e.BlobRef)
}

// UpdateBlob updates the blob data for an existing key.
// Returns ErrMemoryOnly in memory-only mode.
func (db *DB) UpdateBlob(key string, blob []byte) error {
	if db.memoryOnly {
		return ErrMemoryOnly
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	entry, exists := db.data[key]
	if !exists {
		return ErrNotFound
	}

	// Delete old blob if exists
	if entry.BlobRef != "" {
		db.blobs.deleteBlob(entry.BlobRef)
	}

	// Write new blob
	blobRef, err := db.blobs.writeBlob(blob)
	if err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}

	// Track blob for transaction rollback
	if db.inTransaction {
		db.transactionBlobs = append(db.transactionBlobs, blobRef)
	}

	// Update entry with new blob reference
	entry.BlobRef = blobRef

	// Mark dirty for debounced save
	db.markDirty()

	return nil
}

// BeginTransaction starts transaction mode (defers save until Commit).
func (db *DB) BeginTransaction() error {
	db.transactionMu.Lock()
	defer db.transactionMu.Unlock()

	db.mu.RLock()
	defer db.mu.RUnlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	if db.inTransaction {
		return ErrAlreadyInTransaction
	}

	db.inTransaction = true
	db.transactionBlobs = nil
	return nil
}

// Commit exits transaction mode and saves immediately.
func (db *DB) Commit() error {
	db.transactionMu.Lock()
	defer db.transactionMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.checkClosed(); err != nil {
		return err
	}

	if !db.inTransaction {
		return ErrNotInTransaction
	}

	db.inTransaction = false
	db.transactionBlobs = nil // Clear tracked blobs - they're now committed

	// Stop timer and save immediately (skip in memory-only mode)
	if !db.memoryOnly {
		db.stopTimerAndFlush()
	}
	return nil
}

// Rollback exits transaction mode without saving and deletes any blobs created.
func (db *DB) Rollback() error {
	db.transactionMu.Lock()
	defer db.transactionMu.Unlock()

	if !db.inTransaction {
		return ErrNotInTransaction
	}

	// Delete any blobs created during the transaction (if not in memory-only mode)
	if !db.memoryOnly {
		for _, blobRef := range db.transactionBlobs {
			db.blobs.deleteBlob(blobRef)
		}
	}

	db.inTransaction = false
	db.transactionBlobs = nil
	db.dirty = false
	return nil
}

// ttlCleanupLoop runs the TTL cleanup goroutine
func (db *DB) ttlCleanupLoop() {
	defer db.ttlWg.Done()

	ticker := time.NewTicker(db.config.TTLCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.ttlStopCh:
			return
		case <-ticker.C:
			db.cleanupExpired()
		}
	}
}

// cleanupExpired removes expired keys and their blobs
func (db *DB) cleanupExpired() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return
	}

	var deleted bool

	for key, entry := range db.data {
		if entry.isExpired() {
			// Delete blob if exists (and not in memory-only mode)
			if !db.memoryOnly && entry.BlobRef != "" {
				db.blobs.deleteBlob(entry.BlobRef)
			}
			delete(db.data, key)
			deleted = true
		}
	}

	// Mark dirty if any keys were deleted
	if deleted {
		db.markDirty()
	}
}

// cleanupOrphanedBlobs removes blobs on disk that aren't referenced by any key
func (db *DB) cleanupOrphanedBlobs() {
	if db.memoryOnly {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return
	}

	// Build set of referenced blob IDs
	referenced := make(map[string]struct{})
	for _, entry := range db.data {
		if entry.BlobRef != "" {
			referenced[entry.BlobRef] = struct{}{}
		}
	}

	// Delete orphaned blobs
	db.blobs.deleteOrphanedBlobs(referenced)
}
