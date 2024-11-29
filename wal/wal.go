package wal

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huandu/skiplist"
)

type ObjectStore interface {
	Write(data []byte) error
	Read(offset int64, size int) ([]byte, error)
	Sync() error
}

type Options struct {
	AwaitFlush bool
}

type ValueDeletable struct {
	Value    []byte
	IsDelete bool
}

type KVTable struct {
	skl         *skiplist.SkipList
	size        atomic.Int64
	isDurableCh chan bool
}

type WAL struct {
	store           ObjectStore
	mu              sync.RWMutex
	activeTable     *KVTable
	immutableTables []*KVTable
	flushInterval   time.Duration
	stopCh          chan struct{}
}

func NewWAL(store ObjectStore, flushInterval time.Duration) *WAL {
	wal := &WAL{
		store:         store,
		activeTable:   newKVTable(),
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}
	go wal.periodicFlush()
	return wal
}

func newKVTable() *KVTable {
	return &KVTable{
		skl:         skiplist.New(skiplist.BytesAsc),
		isDurableCh: make(chan bool),
	}
}

func (w *WAL) Put(k []byte, v []byte, opts Options) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	oldSize := len(k) + len(w.activeTable.skl.Get(k).Value.(ValueDeletable).Value)
	w.activeTable.skl.Set(k, ValueDeletable{Value: v, IsDelete: false})
	newSize := len(k) + len(v)
	w.activeTable.size.Add(int64(newSize - oldSize))

	if opts.AwaitFlush {
		<-w.activeTable.isDurableCh
	}

	return nil
}

func (w *WAL) Delete(k []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	oldSize := len(k) + len(w.activeTable.skl.Get(k).Value.(ValueDeletable).Value)
	w.activeTable.skl.Set(k, ValueDeletable{IsDelete: true})
	w.activeTable.size.Add(int64(len(k) - oldSize))

	<-w.activeTable.isDurableCh
	return nil
}

func (w *WAL) Get(key []byte) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Check active table first
	if value := w.activeTable.skl.Get(key); value != nil {
		vd := value.Value.(ValueDeletable)
		if vd.IsDelete {
			return nil, errors.New("key not found")
		}
		return vd.Value, nil
	}

	// Check immutable tables in reverse order
	for i := len(w.immutableTables) - 1; i >= 0; i-- {
		if value := w.immutableTables[i].skl.Get(key); value != nil {
			vd := value.Value.(ValueDeletable)
			if vd.IsDelete {
				return nil, errors.New("key not found")
			}
			return vd.Value, nil
		}
	}

	return nil, errors.New("key not found")
}

func (w *WAL) periodicFlush() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flushActiveTable()
		case <-w.stopCh:
			return
		}
	}
}

func (w *WAL) flushActiveTable() {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Create a new active table
	newActiveTable := newKVTable()

	// Move the current active table to immutable tables
	immutableTable := w.activeTable
	w.immutableTables = append(w.immutableTables, immutableTable)
	w.activeTable = newActiveTable

	// Start a goroutine to flush the immutable table to object store
	go w.flushTableToObjectStore(immutableTable)
}

func (w *WAL) flushTableToObjectStore(table *KVTable) {
	// Implement the logic to serialize the table and write it to the object store
	serializedData := serializeKVTable(table)
	err := w.store.Write(serializedData)
	if err != nil {
		// Handle error (you might want to implement a retry mechanism)
		return
	}

	err = w.store.Sync()
	if err != nil {
		// Handle error
		return
	}

	// Notify waiting clients that the table is durable
	close(table.isDurableCh)
}

func serializeKVTable(table *KVTable) []byte {
	// Implement the logic to serialize the KVTable
	// This is a placeholder implementation
	serialized := make([]byte, 0)
	for iter := table.skl.Front(); iter != nil; iter = iter.Next() {
		key := iter.Key().([]byte)
		value := iter.Value.(ValueDeletable)
		// Append key-value pair to serialized data
		// You'll need to implement a proper serialization format
		serialized = append(serialized, key...)
		serialized = append(serialized, value.Value...)
	}
	return serialized
}

func (w *WAL) Close() {
	close(w.stopCh)
	// Implement any necessary cleanup
}
