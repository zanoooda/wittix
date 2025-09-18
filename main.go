package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strconv"
    "syscall"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

type Job struct {
    ID        string    `json:"id" bson:"_id"`
    Payload   string    `json:"payload" bson:"payload"`
    Timeout   int       `json:"timeout" bson:"timeout"` // seconds
    MaxRetry  int       `json:"maxRetry" bson:"maxRetry"`
    CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
}

type JobResult struct {
    ID         string    `json:"id" bson:"_id"`
    Success    bool      `json:"success" bson:"success"`
    Attempts   int       `json:"attempts" bson:"attempts"`
    Err        string    `json:"err,omitempty" bson:"err,omitempty"`
    Started    time.Time `json:"started" bson:"startedAt"`
    Ended      time.Time `json:"ended" bson:"endedAt"`
    DurationMS int64     `json:"duration_ms" bson:"duration_ms"`
}

func getenv(k, def string) string { if v := os.Getenv(k); v != "" { return v }; return def }
func getenvInt(k string, def int) int {
    if v := os.Getenv(k); v != "" {
        if n, err := strconv.Atoi(v); err == nil { return n }
    }
    return def
}
func getenvDur(k string, def time.Duration) time.Duration {
    if v := os.Getenv(k); v != "" {
        if d, err := time.ParseDuration(v); err == nil { return d }
    }
    return def
}

func main() {
    // Config
    uri := getenv("MONGO_URI", "mongodb://localhost:27017")
    dbName := getenv("MONGO_DB", "jobsdb")
    collName := getenv("MONGO_COLL", "jobs")
    addr := getenv("HTTP_ADDR", ":8080")

    workers := getenvInt("WORKERS", 4)
    queueSize := getenvInt("QUEUE_SIZE", 64)
    defTimeout := getenvDur("JOB_DEFAULT_TIMEOUT", 30*time.Second)
    baseBackoff := getenvDur("BACKOFF_BASE", 200*time.Millisecond)
    maxBackoff := getenvDur("BACKOFF_MAX", 5*time.Second)

    // Mongo
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
    if err != nil { log.Fatalf("mongo connect: %v", err) }
    if err := cli.Ping(ctx, nil); err != nil { log.Fatalf("mongo ping: %v", err) }
    coll := cli.Database(dbName).Collection(collName)

    // Runner
    runner := NewRunner(RunnerConfig{
        QueueSize:      queueSize,
        Workers:        workers,
        DefaultTimeout: defTimeout,
        BaseBackoff:    baseBackoff,
        MaxBackoff:     maxBackoff,
        JitterFraction: 0.2,
        OnResult: func(ctx context.Context, res JobResult) {
            // Update existing job doc with result fields
            _, _ = coll.UpdateByID(ctx, res.ID, bson.M{
                "$set": bson.M{
                    "success":     res.Success,
                    "attempts":    res.Attempts,
                    "err":         res.Err,
                    "startedAt":   res.Started,
                    "endedAt":     res.Ended,
                    "duration_ms": res.DurationMS,
                    "updatedAt":   time.Now(),
                },
            })
        },
    }, defaultProcessor)
    runner.Start()
    defer runner.Close()

    // HTTP
    mux := http.NewServeMux()

    // POST /jobs → insert into Mongo and enqueue
    mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        var in Job
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
            http.Error(w, "invalid JSON", http.StatusBadRequest)
            return
        }
        if in.ID == "" { in.ID = primitive.NewObjectID().Hex() }
        if in.MaxRetry < 0 { in.MaxRetry = 0 }
        in.CreatedAt = time.Now()

        ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
        defer cancel()
        if _, err := coll.InsertOne(ctx, bson.M{
            "_id":       in.ID,
            "payload":   in.Payload,
            "timeout":   in.Timeout,
            "maxRetry":  in.MaxRetry,
            "createdAt": in.CreatedAt,
        }); err != nil {
            http.Error(w, "db insert failed", http.StatusInternalServerError)
            return
        }
        if err := runner.Enqueue(ctx, in); err != nil {
            if err == ErrClosed {
                http.Error(w, "runner closed", http.StatusServiceUnavailable)
                return
            }
            http.Error(w, "enqueue timeout", http.StatusGatewayTimeout)
            return
        }
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusAccepted)
        _ = json.NewEncoder(w).Encode(in)
    })

    // GET /stats
    mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(runner.Stats())
    })

    mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

    srv := &http.Server{Addr: addr, Handler: mux}

    // Serve + graceful shutdown
    go func() {
        log.Printf("HTTP listening on %s", addr)
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("http server: %v", err)
        }
    }()
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
    <-stop
    log.Printf("shutting down...")

    shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer shCancel()
    _ = srv.Shutdown(shCtx)
    runner.Close()
    _ = cli.Disconnect(shCtx)
    log.Printf("clean shutdown complete")
}