package stream

import (
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSeq_Parallel_WorkersRunConcurrently(t *testing.T) {
	const n = 4
	var wg sync.WaitGroup
	wg.Add(n)
	rendezvous := func(x int) int {
		wg.Done()
		wg.Wait()
		return x
	}

	done := make(chan []int, 1)
	go func() { done <- SeqOf(1, 2, 3, 4).Parallel(n, rendezvous).Collect() }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Parallel never finished — workers did not run concurrently")
	}
}

func TestSeq_Parallel_ProcessesEveryItem(t *testing.T) {
	got := SeqOf(1, 2, 3, 4, 5).Parallel(3, func(n int) int { return n * n }).Collect()
	sort.Ints(got)
	want := []int{1, 4, 9, 16, 25}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Seq.Parallel().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_Parallel_IsLazy(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	s := SeqOf(1, 2, 3).Parallel(2, func(n int) int {
		mu.Lock()
		calls++
		mu.Unlock()
		return n
	})
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Errorf("Seq.Parallel ran fn %d times before iteration, want 0", calls)
	}
	_ = s
}

func TestSeq_Parallel_StopsEarlyOnInfiniteSource(t *testing.T) {
	naturals := FromSeq(func(yield func(int) bool) {
		for i := 0; yield(i); i++ {
		}
	})
	done := make(chan []int, 1)
	go func() { done <- naturals.Parallel(2, func(n int) int { return n }).Take(3).Collect() }()
	select {
	case got := <-done:
		if len(got) != 3 {
			t.Errorf("Take(3) after Parallel returned %d items, want 3", len(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Seq.Parallel did not stop after the consumer stopped")
	}
}
