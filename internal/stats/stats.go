package stats

import (
	"maps"
	"sync"
	"sync/atomic"
)

type stats struct {
	mu         sync.RWMutex
	success    atomic.Int64
	fails      atomic.Int64
	workerJobs map[int]int
}

// NewStats конструктор
func NewStats() *stats {
	return &stats{
		workerJobs: make(map[int]int),
	}
}

// IncFail увеличивает общее количество не успеха
func (s *stats) IncFail() {
	s.fails.Add(1)
}

// FailCount получает общее количество не успеха
func (s *stats) FailCount() int64 {
	return s.fails.Load()
}

// IncSuccess увеличивает общее количество успехов
func (s *stats) IncSuccess() {
	s.success.Add(1)
}

// SuccessCount получает общее количество успехов
func (s *stats) SuccessCount() int64 {
	return s.success.Load()
}

// IncWorkerJobs уыеличивает успешную задачу на 1 для конкретного воркера
func (s *stats) IncWorkerJobs(workerID int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workerJobs[workerID] += 1
	s.IncSuccess()
}

// WorkerJobCounts получает мапу: количество успешных задач по воркерам
func (s *stats) WorkerJobCounts() map[int]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[int]int, len(s.workerJobs))
	maps.Copy(out, s.workerJobs)

	return out
}
