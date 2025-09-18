package main

import (
    "context"
    "errors"
    "log"
    "math/rand"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

type Processor func(ctx context.Context, j Job) error

type RunnerConfig struct {
    QueueSize      int
    Workers        int
    DefaultTimeout time.Duration
    BaseBackoff    time.Duration
    MaxBackoff     time.Duration
    JitterFraction float64

	OnResult func(ctx context.Context, res JobResult)
}

type Runner struct {
    cfg    RunnerConfig
    proc   Processor
    jobs   chan Job
    wg     sync.WaitGroup
    closed chan struct{}
    once   sync.Once

    accepted  int64
    inFlight  int64
    succeeded int64
    failed    int64
    retried   int64
}

var ErrClosed = errors.New("runner closed")

func NewRunner(cfg RunnerConfig, p Processor) *Runner {
    if cfg.QueueSize <= 0 { cfg.QueueSize = 64 }
    if cfg.Workers <= 0 { cfg.Workers = 4 }
    if cfg.DefaultTimeout <= 0 { cfg.DefaultTimeout = 30 * time.Second }
    if cfg.BaseBackoff <= 0 { cfg.BaseBackoff = 200 * time.Millisecond }
    if cfg.MaxBackoff <= 0 { cfg.MaxBackoff = 5 * time.Second }
    if cfg.JitterFraction < 0 { cfg.JitterFraction = 0.2 }
    return &Runner{
        cfg:    cfg,
        proc:   p,
        jobs:   make(chan Job, cfg.QueueSize),
        closed: make(chan struct{}),
    }
}

func (r *Runner) Start() {
    for i := 0; i < r.cfg.Workers; i++ {
        r.wg.Add(1)
        go r.worker(i)
    }
}

func (r *Runner) Close() {
    r.once.Do(func() {
        close(r.closed)
        close(r.jobs)
    })
    r.wg.Wait()
}

func (r *Runner) Enqueue(ctx context.Context, j Job) error { // first check if closed, then try to enqueue
    select {
    case <-r.closed:
        return ErrClosed
    default:
    }
    select {
    case r.jobs <- j:
        atomic.AddInt64(&r.accepted, 1)
        return nil
    case <-r.closed:
        return ErrClosed
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (r *Runner) worker(id int) {
    defer r.wg.Done()
    for j := range r.jobs {
        atomic.AddInt64(&r.inFlight, 1)

        started := time.Now()
        timeout := r.cfg.DefaultTimeout
        if j.Timeout > 0 {
            timeout = time.Duration(j.Timeout) * time.Second
        }
        ctx, cancel := context.WithTimeout(context.Background(), timeout)
        attempts, err := r.runWithRetry(ctx, j)
        cancel()
        ended := time.Now()

        if err != nil {
            atomic.AddInt64(&r.failed, 1)
            log.Printf("job %s failed: %v", j.ID, err)
        } else {
            atomic.AddInt64(&r.succeeded, 1)
        }

        // Emit result to sink (e.g., Mongo) if configured
        if r.cfg.OnResult != nil {
            res := JobResult{
                ID:         j.ID,
                Success:    err == nil,
                Attempts:   attempts,
                Err:        errString(err),
                Started:    started,
                Ended:      ended,
                DurationMS: ended.Sub(started).Milliseconds(),
            }
            saveCtx, cancelSave := context.WithTimeout(context.Background(), 3*time.Second)
            r.cfg.OnResult(saveCtx, res)
            cancelSave()
        }

        atomic.AddInt64(&r.inFlight, -1)
    }
}

func errString(err error) string {
    if err == nil { return "" }
    return err.Error()
}

func (r *Runner) runWithRetry(ctx context.Context, j Job) (int, error) {
    attempt := 0
    for {
        err := r.proc(ctx, j)
        if err == nil {
            return attempt + 1, nil
        }
        if ctx.Err() != nil {
            return attempt + 1, ctx.Err()
        }
        if attempt >= j.MaxRetry {
            return attempt + 1, err
        }
        attempt++
        atomic.AddInt64(&r.retried, 1)

        back := r.cfg.BaseBackoff << (attempt - 1)
        if back > r.cfg.MaxBackoff {
            back = r.cfg.MaxBackoff
        }
        jit := time.Duration((rand.Float64()*2 - 1) * r.cfg.JitterFraction * float64(back))
        back += jit

        select {
        case <-time.After(back):
        case <-r.closed:
            return attempt + 1, ErrClosed
        case <-ctx.Done():
            return attempt + 1, ctx.Err()
        }
    }
}

type RunnerStats struct {
    Accepted  int64 `json:"accepted"`
    InFlight  int64 `json:"inFlight"`
    Succeeded int64 `json:"succeeded"`
    Failed    int64 `json:"failed"`
    Retried   int64 `json:"retried"`
    QueueLen  int   `json:"queueLen"`
    QueueCap  int   `json:"queueCap"`
    Workers   int   `json:"workers"`
}

func (r *Runner) Stats() RunnerStats {
    return RunnerStats{
        Accepted:  atomic.LoadInt64(&r.accepted),
        InFlight:  atomic.LoadInt64(&r.inFlight),
        Succeeded: atomic.LoadInt64(&r.succeeded),
        Failed:    atomic.LoadInt64(&r.failed),
        Retried:   atomic.LoadInt64(&r.retried),
        QueueLen:  len(r.jobs),
        QueueCap:  cap(r.jobs),
        Workers:   r.cfg.Workers,
    }
}

// Demo processor controlled by payload:
// "sleep:2s" sleeps; "fail" forces error; else succeeds after 100ms.
func defaultProcessor(ctx context.Context, j Job) error {
    if strings.HasPrefix(j.Payload, "sleep:") {
        d, err := time.ParseDuration(strings.TrimPrefix(j.Payload, "sleep:"))
        if err != nil { d = 500 * time.Millisecond }
        select {
        case <-time.After(d):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    if strings.HasPrefix(j.Payload, "fail") {
        return errors.New("forced failure")
    }
    select {
    case <-time.After(100 * time.Millisecond):
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}