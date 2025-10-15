// Package diskview provides a low-level abstraction for managing and reading
// fixed-size pages stored on disk. It includes a built-in caching layer that
// transparently keeps frequently accessed pages in memory while handling
// eviction, reads, and writes efficiently.
//
// The benchmarks in this file measure various real-world access patterns and
// cache behaviors, including pure cache hits, misses, sequential and random
// reads, eviction stress, GC effects, and concurrent workloads. Together, they
// help validate cache performance, concurrency safety, and overall disk I/O
// efficiency across different usage patterns.
package diskview

import (
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

var pageSize int64 = int64(os.Getpagesize())

// setup initializes a temporary DiskViewer instance for benchmarking.
// It creates a new temporary directory and data file with the given cache capacity.
func setup(b *testing.B, capacity int) *DiskViewer {
	dir := b.TempDir()
	file := filepath.Join(dir, "bench.data")

	view, err := New(file, Config{MaxCapacity: capacity})
	if err != nil {
		b.Fatal(err)
	}
	return view
}

// BenchmarkRead_100PercentHitRate measures read performance when
// every requested page is already cached in memory.
// This simulates the best-case scenario with zero disk I/O.
func BenchmarkRead_100PercentHitRate(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 100)
	defer view.Close()

	// create ids
	ids := make([]int64, 100)
	for i := range 100 {
		if id, err := view.Create(); err != nil {
			b.Fatal(err)
		} else {
			ids[i] = id
		}
	}

	// warm up cache
	for _, page := range ids {
		_, _ = view.Read(page)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%len(ids)]
		_, _ = view.Read(id)
	}
}

// BenchmarkRead_100PercentMissRate measures performance when
// every read results in a cache miss and requires loading from disk.
// This represents the worst-case read scenario.
func BenchmarkRead_100PercentMissRate(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 10)
	defer view.Close()

	ids := make([]int64, 10_000)
	for i := range 10_000 {
		if id, err := view.Create(); err != nil {
			b.Fatal(err)
		} else {
			ids[i] = id
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%len(ids)]
		_, _ = view.Read(id)
	}
}

// BenchmarkRead_80PercentHitRate measures a mixed workload where
// roughly 80% of reads hit the cache and 20% miss.
// This simulates a realistic steady-state cache efficiency.
func BenchmarkRead_80PercentHitRate(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	cacheSize := 1000
	view := setup(b, cacheSize)
	defer view.Close()

	ids := make([]int64, 5000)
	for i := range 5000 {
		if id, err := view.Create(); err != nil {
			b.Fatal(err)
		} else {
			ids[i] = id
		}
	}

	// warm up cache
	for i := range cacheSize {
		id := ids[i]
		_, _ = view.Read(id)
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r.Float64() < 0.8 {
			id := int64(r.Intn(cacheSize)) // cache hit
			_, _ = view.Read(id)
		} else {
			id := int64(cacheSize + r.Intn(5000-cacheSize)) // cache miss
			_, _ = view.Read(id)
		}
	}
}

// BenchmarkRead_SequentialAccess measures performance for
// sequentially reading pages in order. This tests spatial locality.
func BenchmarkRead_SequentialAccess(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 10_000) // enough cache to hold all pages
	defer view.Close()

	ids := make([]int64, 10_000)
	for i := range 10_000 {
		if id, err := view.Create(); err != nil {
			b.Fatal(err)
		} else {
			ids[i] = id
		}
	}

	// warm up cache
	for i := range 10_000 {
		_, _ = view.Read(ids[i])
	}

	b.ResetTimer()
	for i := range b.N {
		id := int64(ids[i%9999])
		_, _ = view.Read(id)
	}
}

// BenchmarkRead_RandomAccess measures performance when pages
// are read in random order. This tests cache adaptability and latency.
func BenchmarkRead_RandomAccess(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 10_000)
	defer view.Close()

	for range 10_000 {
		_, _ = view.Create()
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for range b.N {
		id := int64(r.Intn(10_000))
		_, _ = view.Read(id)
	}
}

// BenchmarkRead_HighEvictionPressure simulates reads with a small cache
// and large working set, causing frequent evictions.
func BenchmarkRead_HighEvictionPressure(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 100) // small cache
	defer view.Close()

	for range 10_000 {
		_, _ = view.Create()
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for range b.N {
		id := int64(r.Intn(10_000))
		_, _ = view.Read(id)
	}
}

// BenchmarkRead_TinyCache measures performance when the cache
// is extremely small, almost guaranteeing misses.
func BenchmarkRead_TinyCache(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 2)
	defer view.Close()

	for range 100 {
		_, _ = view.Create()
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for range b.N {
		id := int64(r.Intn(100))
		_, _ = view.Read(id)
	}
}

// BenchmarkGCStress forces a garbage collection periodically while reading.
// It tests the system's resilience and performance stability under GC load.
func BenchmarkGCStress(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 100)
	defer view.Close()

	for range 1000 {
		_, _ = view.Create()
	}

	b.ResetTimer()
	for i := range b.N {
		id := int64(i % 1000)
		_, _ = view.Read(id)
		if i%1000 == 0 {
			runtime.GC()
		}
	}
}

// BenchmarkReadHeavy simulates a mixed workload with 80% reads and 20% writes.
// It models real-world scenarios like analytics workloads.
func BenchmarkReadHeavy(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 1000)
	defer view.Close()

	for range 10_000 {
		_, _ = view.Create()
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for range b.N {
		if r.Float64() < 0.8 {
			id := int64(r.Intn(10_000))
			_, _ = view.Read(id)
		} else {
			_, _ = view.Create()
		}
	}
}

// BenchmarkWriteHeavy simulates a mixed workload with 80% writes and 20% reads.
// This tests write throughput and fsync efficiency.
func BenchmarkWriteHeavy(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 1000)
	defer view.Close()

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for range b.N {
		if r.Float64() < 0.2 {
			id := int64(r.Intn(10_000))
			_, _ = view.Read(id)
		} else {
			_, _ = view.Create()
		}
	}
}

// BenchmarkConcurrent_MixedReadWrite tests parallel mixed read/write workloads
// across multiple goroutines to evaluate contention and thread safety.
func BenchmarkConcurrent_MixedReadWrite(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(pageSize)

	view := setup(b, 1000)
	defer view.Close()

	for i := int64(0); i < 10_000; i++ {
		_, _ = view.Create()
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		src := rand.NewSource(time.Now().UnixNano())
		r := rand.New(src)
		for pb.Next() {
			if r.Float64() < 0.7 {
				id := int64(r.Intn(10_000))
				_, _ = view.Read(id)
			} else {
				_, _ = view.Create()
			}
		}
	})
}
