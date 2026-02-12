package workers

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sharelib-jp/sharelib/lib"
)

var (
	ErrInvalidBufferSize  = errors.New("invalid buffer size")
	ErrInvalidWorkerCount = errors.New("invalid worker count")
	ErrPoolStopped        = errors.New("priority pool stopped")
)

var (
	jobIdConfig = lib.SnowflakeBitConfig{
		PrefixBits:    1,
		TimeStampBits: 41,
		MachineIDBits: 10,
		SequenceBits:  12,
		Prefix:        0,
		MeasureTime:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		MachineID:     1,
	}
)

type Job struct {
	ID       lib.SnowflakeId
	Time     time.Time
	TimeOut  time.Duration
	Fn       func()
	Comment  string
	Priority string
	Status   uint8 // 0b0000: queued, 0b0001: running, 0b0010: completed, 0b0100: failed
}

type PriorityPool struct {
	highChan      chan Job
	lowChan       chan Job
	wg            sync.WaitGroup
	quit          chan struct{}
	mu            sync.RWMutex
	stopped       bool
	sequence      uint16
	lastTimestamp time.Time
	// queuedJobs holds jobs that have been submitted but not yet taken by workers.
	// Access must be protected by p.mu.
	queuedJobs []Job
}

func NewPriorityPool(bufferSize int) (*PriorityPool, error) {
	if bufferSize <= 0 {
		return nil, ErrInvalidBufferSize
	}
	return &PriorityPool{
		highChan: make(chan Job, bufferSize),
		lowChan:  make(chan Job, bufferSize),
		quit:     make(chan struct{}),
	}, nil
}

func (p *PriorityPool) Start(n int) error {
	if n <= 0 {
		return ErrInvalidWorkerCount
	}
	p.mu.RLock()
	stopped := p.stopped
	p.mu.RUnlock()
	if stopped {
		return ErrPoolStopped
	}

	for i := range make([]int, n) {
		p.wg.Add(1)
		go p.worker(i)
	}
	return nil
}

func (p *PriorityPool) worker(id int) {
	defer p.wg.Done()
	slog.Info("Worker started", slog.Int("workerID", id))

	for {
		select {
		case job := <-p.highChan:
			p.runJob(id, "High", job)
			continue
		default:
		}

		select {
		case job := <-p.highChan:
			p.runJob(id, "High", job)
		case job := <-p.lowChan:
			p.runJob(id, "Low", job)
		case <-p.quit:
			slog.Info("Worker stopping", slog.Int("workerID", id))
			return
		}
	}
}

func (p *PriorityPool) runJob(workerID int, priority string, job Job) {
	slog.Info("Job started",
		slog.Int64("jobID", job.ID.Int64()),
		slog.String("priority", priority),
		slog.String("comment", job.Comment),
		slog.String("queuedAt", job.Time.Format(time.RFC3339)),
		slog.Duration("timeout", job.TimeOut),
	)

	if job.Fn == nil {
		job.Status = 0b0100 // failed
		return
	}

	job.Status = 0b0001 // running

	if job.TimeOut > 0 {
		done := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					job.Status = 0b0100 // failed
					slog.Error("Job failed with panic", slog.Any("recover", r))
				}
			}()
			job.Fn()
			close(done)
		}()

		select {
		case <-done:
			job.Status = 0b0010 // completed
			slog.Info("Job completed", slog.Int64("jobID", job.ID.Int64()))
		case <-time.After(job.TimeOut):
			slog.Warn("Job timed out", slog.Int64("jobID", job.ID.Int64()), slog.String("priority", priority))
			job.Status = 0b0100 // failed
		}
		return
	}

	defer func() {
		if r := recover(); r != nil {
			job.Status = 0b0100 // failed
			slog.Error("Job failed with panic", slog.Any("recover", r))
		}
	}()

	job.Fn()
	job.Status = 0b0010 // completed
	slog.Info("Job completed", slog.Int64("jobID", job.ID.Int64()))
}

func (p *PriorityPool) SubmitHigh(job Job) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPoolStopped
	}

	// Generate unique Snowflake ID with incrementing sequence (requires lock)
	id, err := p.generateJobID()
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to generate Snowflake ID: %w", err)
	}

	job.ID = id
	job.Time = time.Now()
	job.Priority = "High"

	// Track queued jobs separately to allow safe snapshots without draining channels.
	p.queuedJobs = append(p.queuedJobs, job)
	p.mu.Unlock()

	p.highChan <- job
	return nil
}

func (p *PriorityPool) SubmitLow(job Job) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPoolStopped
	}

	// Generate unique Snowflake ID with incrementing sequence (requires lock)
	id, err := p.generateJobID()
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to generate Snowflake ID: %w", err)
	}

	job.ID = id
	job.Time = time.Now()
	job.Priority = "Low"

	// Track queued jobs separately to allow safe snapshots without draining channels.
	p.queuedJobs = append(p.queuedJobs, job)
	p.mu.Unlock()

	p.lowChan <- job
	return nil
}

func (p *PriorityPool) ListJobs() ([]Job, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		return nil, ErrPoolStopped
	}

	// Return a copy of the queued jobs snapshot maintained in-memory.
	out := make([]Job, len(p.queuedJobs))
	copy(out, p.queuedJobs)
	return out, nil
}

// removeQueuedJob removes the first queued job with the given ID.
func (p *PriorityPool) removeQueuedJob(id lib.SnowflakeId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, j := range p.queuedJobs {
		if j.ID.Int64() == id.Int64() {
			p.queuedJobs = append(p.queuedJobs[:i], p.queuedJobs[i+1:]...)
			return
		}
	}
}

// generateJobID generates a unique Snowflake ID with monotonic sequence.
// Must be called with p.mu locked.
func (p *PriorityPool) generateJobID() (lib.SnowflakeId, error) {
	now := time.Now()
	currentMs := now.Truncate(time.Millisecond)

	// Check if we're in the same millisecond
	if currentMs.Equal(p.lastTimestamp) {
		p.sequence++
		// Check for sequence overflow
		maxSequence := uint16((1 << jobIdConfig.SequenceBits) - 1)
		if p.sequence > maxSequence {
			// Wait for next millisecond
			time.Sleep(time.Millisecond)
			now = time.Now()
			currentMs = now.Truncate(time.Millisecond)
			p.sequence = 0
		}
	} else {
		// New millisecond, reset sequence
		p.sequence = 0
	}

	p.lastTimestamp = currentMs

	return lib.GenerateSnowflakeID(jobIdConfig, now, p.sequence)
}

func (p *PriorityPool) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	close(p.quit)
	p.wg.Wait()
}
