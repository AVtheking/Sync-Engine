package main

import (
	"log"
	"sync"
)

type LogEntries struct {
	Offset int64             `json:"offset"`
	Ops    string            `json:"ops"`
	Key    string            `json:"key"`
	Value  map[string]string `json:"value"`
}

type ShapeLog struct {
	mu      sync.Mutex
	entries []LogEntries
}

type ShapeRegistry struct {
	mu     sync.Mutex
	shapes map[string]*ShapeLog // relation name -> shape log
}

func NewShapeRegistry() *ShapeRegistry {
	return &ShapeRegistry{
		shapes: make(map[string]*ShapeLog),
	}
}

func (r *ShapeRegistry) GetOrCreate(table string) *ShapeLog {
	r.mu.Lock()
	defer r.mu.Unlock()

	shape, ok := r.shapes[table]
	if !ok {
		shape = &ShapeLog{}
		r.shapes[table] = shape
	}

	return shape
}

func (s *ShapeLog) AppendEntries(entries []LogEntries) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		entry.Offset = int64(len(s.entries))
		s.entries = append(s.entries, entry)
	}
	log.Printf("SHAPE LOG: appended %d entries atomically, log now has %d total", len(entries), len(s.entries))
}

func (s *ShapeLog) ReadFrom(offset int64) ([]LogEntries, int64) {
	log.Printf("SHAPE LOG: reading from offset %d", offset)
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset >= int64(len(s.entries)) {
		return []LogEntries{}, int64(len(s.entries))
	}

	entries := make([]LogEntries, len(s.entries)-int(offset))
	copy(entries, s.entries[offset:])
	return entries, offset + int64(len(entries))
}
