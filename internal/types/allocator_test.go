package types

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocatorNextIsMonotonic(t *testing.T) {
	a := NewDocIDAllocator()
	require.Equal(t, DocID(1), a.Next())
	require.Equal(t, DocID(2), a.Next())
	require.Equal(t, DocID(3), a.Next())
}

func TestAllocatorObserveTakesMax(t *testing.T) {
	a := NewDocIDAllocator()
	a.Observe(100)
	a.Observe(50) // smaller, should not regress
	a.Observe(200)
	a.Observe(150) // smaller, no regression
	require.Equal(t, DocID(201), a.Next())
}

func TestAllocatorObserveAfterNext(t *testing.T) {
	a := NewDocIDAllocator()
	require.Equal(t, DocID(1), a.Next())
	require.Equal(t, DocID(2), a.Next())
	a.Observe(5) // recovered SSTable claims it saw DocID(5)
	require.Equal(t, DocID(6), a.Next())
}

func TestAllocatorPeek(t *testing.T) {
	a := NewDocIDAllocator()
	require.Equal(t, DocID(0), a.Peek())
	_ = a.Next()
	require.Equal(t, DocID(1), a.Peek())
	a.Observe(99)
	require.Equal(t, DocID(99), a.Peek())
}

func TestAllocatorConcurrentNextNoDuplicates(t *testing.T) {
	a := NewDocIDAllocator()
	const goroutines = 16
	const perG = 1000

	seen := sync.Map{}
	var dupes atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				id := a.Next()
				if _, loaded := seen.LoadOrStore(id, struct{}{}); loaded {
					dupes.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Zero(t, dupes.Load())
	require.Equal(t, DocID(goroutines*perG), a.Peek())
}

func TestAllocatorConcurrentNextAndObserve(t *testing.T) {
	a := NewDocIDAllocator()
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			a.Next()
		}
	}()
	go func() {
		defer wg.Done()
		for i := DocID(1); i <= 10000; i++ {
			a.Observe(i)
		}
	}()
	wg.Wait()

	// After completion, Peek should be at least 10000 (largest Observe) and
	// the next Next() should be strictly larger.
	peek := a.Peek()
	require.GreaterOrEqual(t, peek, DocID(10000))
	require.Greater(t, a.Next(), peek)
}
