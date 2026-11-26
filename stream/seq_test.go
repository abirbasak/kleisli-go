package stream

import (
	"maps"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/azuiktech/kleisli-go/adt"
)

func TestFromSeq_Collect_RoundTrips(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3})).Collect()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Collect() = %v, want %v", got, want)
	}
}

func TestSeq_Filter(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3, 4, 5, 6})).
		Filter(func(n int) bool { return n%2 == 0 }).
		Collect()
	want := []int{2, 4, 6}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Filter().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_Map(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3})).
		Map(func(n int) string { return string(rune('a' + n - 1)) }).
		Collect()
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_FlatMap(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3})).
		FlatMap(func(n int) []int { return []int{n, n * 10} }).
		Collect()
	want := []int{1, 10, 2, 20, 3, 30}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FlatMap().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_Take_StopsPullingUpstream(t *testing.T) {
	calls := 0
	source := func(yield func(int) bool) {
		for i := 1; i <= 1000; i++ {
			calls++
			if !yield(i) {
				return
			}
		}
	}
	got := FromSeq(source).Take(3).Collect()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Take(3).Collect() = %v, want %v", got, want)
	}
	if calls != 3 {
		t.Errorf("source produced %d items, want exactly 3 (Take must stop pulling upstream)", calls)
	}
}

func TestSeq_Skip(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3, 4, 5})).Skip(2).Collect()
	want := []int{3, 4, 5}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Skip(2).Collect() = %v, want %v", got, want)
	}
}

func TestSeq_ForEach(t *testing.T) {
	var got []int
	FromSeq(slices.Values([]int{1, 2, 3})).ForEach(func(n int) { got = append(got, n) })
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ForEach() collected %v, want %v", got, want)
	}
}

func TestSeq_Tap(t *testing.T) {
	var tapped []int
	got := FromSeq(slices.Values([]int{1, 2, 3})).
		Tap(func(n int) { tapped = append(tapped, n) }).
		Map(func(n int) int { return n * 2 }).
		Collect()

	if !reflect.DeepEqual(tapped, []int{1, 2, 3}) {
		t.Errorf("Tap() collected %v, want [1 2 3]", tapped)
	}
	if !reflect.DeepEqual(got, []int{2, 4, 6}) {
		t.Errorf("Map after Tap() = %v, want [2 4 6]", got)
	}
}

func TestSeq_AnyOf_ShortCircuits(t *testing.T) {
	calls := 0
	source := func(yield func(int) bool) {
		for i := 1; i <= 1000; i++ {
			calls++
			if !yield(i) {
				return
			}
		}
	}
	got := FromSeq(source).AnyOf(func(n int) bool { return n == 3 })
	if !got {
		t.Error("AnyOf() = false, want true")
	}
	if calls != 3 {
		t.Errorf("source produced %d items, want exactly 3 (AnyOf must stop at the match)", calls)
	}

	empty := EmptySeq[int]()
	if empty.AnyOf(func(n int) bool { return true }) {
		t.Error("EmptySeq AnyOf must be false")
	}
}

func TestSeq_AllOf(t *testing.T) {
	got := FromSeq(slices.Values([]int{2, 4, 6})).AllOf(func(n int) bool { return n%2 == 0 })
	if !got {
		t.Error("AllOf() = false, want true")
	}
	got = FromSeq(slices.Values([]int{2, 4, 5})).AllOf(func(n int) bool { return n%2 == 0 })
	if got {
		t.Error("AllOf() = true, want false")
	}

	empty := EmptySeq[int]()
	if !empty.AllOf(func(n int) bool { return false }) {
		t.Error("EmptySeq AllOf must be vacuously true")
	}
}

func TestSeq_NoneOf(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 3, 5})).NoneOf(func(n int) bool { return n%2 == 0 })
	if !got {
		t.Error("NoneOf() = false, want true")
	}
	got = FromSeq(slices.Values([]int{1, 3, 4})).NoneOf(func(n int) bool { return n%2 == 0 })
	if got {
		t.Error("NoneOf() = true, want false")
	}

	empty := EmptySeq[int]()
	if !empty.NoneOf(func(n int) bool { return true }) {
		t.Error("EmptySeq NoneOf must be true")
	}
}

func TestSeq_All_RangeOverFunc(t *testing.T) {
	seq := SeqOf(10, 20, 30)
	var got []int
	for v := range seq.All() {
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []int{10, 20, 30}) {
		t.Errorf("for v := range seq.All() = %v, want [10 20 30]", got)
	}
}

func TestSeq_First_ShortCircuitsAcrossFilterAndMap(t *testing.T) {
	// The whole point of Seq over Stream: this must NOT process all 1000
	// upstream items just because Filter and Map are chained before First.
	calls := 0
	source := func(yield func(int) bool) {
		for i := 1; i <= 1000; i++ {
			calls++
			if !yield(i) {
				return
			}
		}
	}
	got := FromSeq(source).
		Filter(func(n int) bool { return n%2 == 0 }).
		Map(func(n int) string { return string(rune('0' + n%10)) }).
		First(func(s string) bool { return s == "4" })
	if got.IsNone() || got.MustGet() != "4" {
		t.Fatalf("First() = %v, want Some(\"4\")", got)
	}
	if calls != 4 {
		t.Errorf("source produced %d items, want exactly 4 — Filter/Map/First must short-circuit end to end", calls)
	}
}

func TestSeq_Reduce(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3, 4})).Reduce(0, func(acc, n int) int { return acc + n })
	if got != 10 {
		t.Errorf("Reduce() = %d, want 10", got)
	}
}

func TestSeq_MapMulti(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3, 4})).
		MapMulti(func(n int, emit func(int)) {
			if n%2 == 0 {
				emit(n)
				emit(n)
			}
		}).
		Collect()
	want := []int{2, 2, 4, 4}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MapMulti().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_FilterMap(t *testing.T) {
	got := FromSeq(slices.Values([]string{"1", "x", "3"})).
		FilterMap(func(s string) (int, bool) {
			switch s {
			case "1":
				return 1, true
			case "3":
				return 3, true
			default:
				return 0, false
			}
		}).
		Collect()
	want := []int{1, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FilterMap().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_TakeWhile_StopsPullingUpstream(t *testing.T) {
	calls := 0
	source := func(yield func(int) bool) {
		for _, v := range []int{1, 2, 3, 100, 4, 5} {
			calls++
			if !yield(v) {
				return
			}
		}
	}
	got := FromSeq(source).TakeWhile(func(n int) bool { return n < 10 }).Collect()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TakeWhile().Collect() = %v, want %v", got, want)
	}
	if calls != 4 {
		t.Errorf("source produced %d items, want exactly 4 (stops at 100, never touches 4,5)", calls)
	}
}

func TestSeq_DropWhile(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3, 4, 1})).DropWhile(func(n int) bool { return n < 3 }).Collect()
	want := []int{3, 4, 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DropWhile().Collect() = %v, want %v", got, want)
	}
}

func TestSeq_DistinctBy(t *testing.T) {
	got := FromSeq(slices.Values([]string{"a", "bb", "c", "dd", "eee"})).
		DistinctBy(func(s string) int { return len(s) }).
		Collect()
	want := []string{"a", "bb", "eee"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DistinctBy().Collect() = %v, want %v", got, want)
	}
}

func TestDistinctSeq(t *testing.T) {
	got := DistinctSeq(FromSeq(slices.Values([]int{1, 2, 2, 3, 1}))).Collect()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DistinctSeq().Collect() = %v, want %v", got, want)
	}
}

func TestEnumerateSeq(t *testing.T) {
	got := EnumerateSeq(FromSeq(slices.Values([]string{"a", "b", "c"}))).Collect()
	want := []adt.Indexed[string]{{Index: 0, Value: "a"}, {Index: 1, Value: "b"}, {Index: 2, Value: "c"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("EnumerateSeq().Collect() = %+v, want %+v", got, want)
	}
}

func TestZipSeq(t *testing.T) {
	got := ZipSeq(FromSeq(slices.Values([]int{1, 2, 3})), FromSeq(slices.Values([]string{"a", "b"}))).Collect()
	want := []adt.Pair[int, string]{adt.PairOf(1, "a"), adt.PairOf(2, "b")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ZipSeq().Collect() = %+v, want %+v", got, want)
	}
}

func TestZipSeq3(t *testing.T) {
	got := ZipSeq3(FromSeq(slices.Values([]int{1, 2, 3})), FromSeq(slices.Values([]string{"a", "b"})), FromSeq(slices.Values([]bool{true, false, true}))).Collect()
	want := []adt.Triple[int, string, bool]{adt.TripleOf(1, "a", true), adt.TripleOf(2, "b", false)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ZipSeq3().Collect() = %+v, want %+v", got, want)
	}
}

func TestSeq_Gather_ScanWindowFold(t *testing.T) {
	scanned := FromSeq(slices.Values([]int{1, 2, 3, 4})).Gather(Scan(0, func(acc, n int) int { return acc + n })).Collect()
	if want := []int{1, 3, 6, 10}; !reflect.DeepEqual(scanned, want) {
		t.Errorf("Gather(Scan).Collect() = %v, want %v", scanned, want)
	}

	windowed := FromSeq(slices.Values([]int{1, 2, 3, 4, 5})).Gather(WindowFixed[int](2)).Collect()
	if want := [][]int{{1, 2}, {3, 4}, {5}}; !reflect.DeepEqual(windowed, want) {
		t.Errorf("Gather(WindowFixed).Collect() = %v, want %v", windowed, want)
	}

	folded := FromSeq(slices.Values([]int{1, 2, 3})).Gather(Fold(0, func(acc, n int) int { return acc + n })).Collect()
	if want := []int{6}; !reflect.DeepEqual(folded, want) {
		t.Errorf("Gather(Fold).Collect() = %v, want %v", folded, want)
	}
}

// TestStreamAndSeq_AgreeOnResults proves the shared-implementation
// property directly: Stream and Seq, given the same input and the same
// operation, must produce identical results — that's the whole reason
// they share an engine instead of two independently maintained loops.
func TestStreamAndSeq_AgreeOnResults(t *testing.T) {
	items := []int{5, 3, 8, 1, 9, 2, 7, 4, 6}
	double := func(n int) int { return n * 2 }
	even := func(n int) bool { return n%2 == 0 }

	streamResult := Of(items).Filter(even).Map(double).Collect()
	seqResult := FromSeq(slices.Values(items)).Filter(even).Map(double).Collect()

	if !reflect.DeepEqual(streamResult, seqResult) {
		t.Errorf("Stream result %v != Seq result %v for the same Filter+Map pipeline", streamResult, seqResult)
	}
}

func TestFromSlice_IsOf(t *testing.T) {
	got := FromSlice([]int{1, 2, 3}).Collect()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FromSlice().Collect() = %v, want %v", got, want)
	}
}

func TestToStream_Materializes(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3})).ToStream().Collect()
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("ToStream().Collect() = %v, want %v", got, want)
	}
}

func TestOption_All(t *testing.T) {
	if got := FromSeq(adt.Some(7).All()).Collect(); !reflect.DeepEqual(got, []int{7}) {
		t.Errorf("FromSeq(Some.All()) = %v, want [7]", got)
	}
	if got := FromSeq(adt.None[int]().All()).Collect(); len(got) != 0 {
		t.Errorf("FromSeq(None.All()) = %v, want []", got)
	}

	var values []int
	for v := range adt.Some(42).All() {
		values = append(values, v)
	}
	if !reflect.DeepEqual(values, []int{42}) {
		t.Errorf("for v := range Some(42).All() = %v, want [42]", values)
	}
}

func TestSeqOfMap(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	got := SeqOfMap(m).ToStream().SortBy(func(p adt.Pair[string, int]) string { return p.First() }).Collect()
	want := []adt.Pair[string, int]{adt.PairOf("a", 1), adt.PairOf("b", 2)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SeqOfMap() sorted = %+v, want %+v", got, want)
	}
}

func TestSeq_ToPipe_ProducesAllItems(t *testing.T) {
	got := FromSeq(slices.Values([]int{1, 2, 3})).ToPipe().Collect()
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("Seq.ToPipe().Collect() = %v, want %v", got, want)
	}
}

func TestFlattenSeq(t *testing.T) {
	got := FlattenSeq(FromSeq(slices.Values([][]int{{1, 2}, {3}, {4, 5, 6}}))).Collect()
	if want := []int{1, 2, 3, 4, 5, 6}; !reflect.DeepEqual(got, want) {
		t.Errorf("FlattenSeq() = %v, want %v", got, want)
	}
}

func TestSeq_MinBy(t *testing.T) {
	type item struct {
		Name string
		N    int
	}
	items := []item{{"b", 2}, {"a", 1}, {"c", 3}}

	got := FromSeq(slices.Values(items)).MinBy(func(i item) int { return i.N })
	if got.IsNone() || got.MustGet().N != 1 {
		t.Errorf("Seq.MinBy() = %+v, want Some({a 1})", got)
	}

	if FromSeq(slices.Values([]item{})).MinBy(func(i item) int { return i.N }).IsSome() {
		t.Error("Seq.MinBy() on empty Seq returned Some")
	}
}

func TestSeq_MaxBy(t *testing.T) {
	type item struct {
		Name string
		N    int
	}
	items := []item{{"b", 2}, {"a", 1}, {"c", 3}}

	got := FromSeq(slices.Values(items)).MaxBy(func(i item) int { return i.N })
	if got.IsNone() || got.MustGet().N != 3 {
		t.Errorf("Seq.MaxBy() = %+v, want Some({c 3})", got)
	}

	if FromSeq(slices.Values([]item{})).MaxBy(func(i item) int { return i.N }).IsSome() {
		t.Error("Seq.MaxBy() on empty Seq returned Some")
	}
}

func TestSeq_ForEach_DrainsSequence(t *testing.T) {
	var got []int
	FromSeq(slices.Values([]int{1, 2, 3})).ForEach(func(v int) { got = append(got, v) })
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("ForEach collected %v, want [1 2 3]", got)
	}
}

func TestSeq_Tap_IsLazyAndPassesThrough(t *testing.T) {
	var tapped []int
	got := FromSeq(slices.Values([]int{1, 2, 3})).
		Tap(func(v int) { tapped = append(tapped, v) }).
		Collect()
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("Tap changed values: got %v", got)
	}
	if !reflect.DeepEqual(tapped, []int{1, 2, 3}) {
		t.Errorf("Tap side-effect saw %v, want [1 2 3]", tapped)
	}
}

func TestSeq_ToMapBy_CollisionMergePolicies(t *testing.T) {
	type pair struct {
		Key string
		Val int
	}
	items := SeqOf(pair{"a", 1}, pair{"b", 2}, pair{"a", 3})
	keyVal := func(p pair) (string, int) { return p.Key, p.Val }

	if got := items.ToMapBy(keyVal, KeepFirst); got["a"] != 1 {
		t.Errorf("ToMapBy(KeepFirst)[a] = %d, want 1", got["a"])
	}
	if got := items.ToMapBy(keyVal, KeepLast); got["a"] != 3 {
		t.Errorf("ToMapBy(KeepLast)[a] = %d, want 3", got["a"])
	}
	sum := func(existing, incoming int) int { return existing + incoming }
	if got := items.ToMapBy(keyVal, sum); got["a"] != 4 {
		t.Errorf("ToMapBy(sum)[a] = %d, want 4", got["a"])
	}
}

func TestSeq_MapWhile_StopsAtFirstNone(t *testing.T) {
	got := SeqOf(1, 2, 3, -1, 4, 5).
		MapWhile(func(n int) adt.Option[int] {
			if n < 0 {
				return adt.None[int]()
			}
			return adt.Some(n * 10)
		}).
		Collect()
	want := []int{10, 20, 30}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MapWhile() = %v, want %v (elements after the stop must be excluded)", got, want)
	}
}

func TestSeq_ScanWhile_StopsAtFirstNone(t *testing.T) {
	got := SeqOf(1, 2, 3, -1, 4).
		ScanWhile(0, func(acc, n int) (int, adt.Option[int]) {
			if n < 0 {
				return acc, adt.None[int]()
			}
			acc += n
			return acc, adt.Some(acc)
		}).
		Collect()
	want := []int{1, 3, 6}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ScanWhile() = %v, want %v (elements after the stop must be excluded)", got, want)
	}
}

func TestSeq_MinByCompare(t *testing.T) {
	less := func(a, b int) int { return a - b }

	if got := SeqOf(5, 3, 8, 1, 9).MinByCompare(less); got.IsNone() || got.MustGet() != 1 {
		t.Errorf("MinByCompare() = %v, want Some(1)", got)
	}
	if got := SeqOf[int]().MinByCompare(less); got.IsSome() {
		t.Errorf("MinByCompare() on empty Seq = %v, want None", got)
	}
}

func TestSeq_MinByCompare_TiesKeepFirst(t *testing.T) {
	type item struct {
		Name string
		N    int
	}
	less := func(a, b item) int { return a.N - b.N }
	got := SeqOf(item{"a", 1}, item{"b", 1}, item{"c", 2}).MinByCompare(less)
	if got.IsNone() || got.MustGet().Name != "a" {
		t.Errorf("MinByCompare() tie = %v, want Some(a) (first element must win)", got)
	}
}

func TestSeq_Count(t *testing.T) {
	if got := SeqOf(1, 2, 3, 4).Filter(func(n int) bool { return n > 1 }).Count(); got != 3 {
		t.Errorf("Count() = %d, want 3", got)
	}
	if got := EmptySeq[int]().Count(); got != 0 {
		t.Errorf("EmptySeq().Count() = %d, want 0", got)
	}
}

func TestSeq_Last(t *testing.T) {
	got := SeqOf(1, 2, 3, 2, 1).Map(func(n int) int { return n * 10 }).Last(func(n int) bool { return n == 20 })
	if got.IsNone() || got.MustGet() != 20 {
		t.Errorf("Last() = %v, want Some(20)", got)
	}
	idx := EnumerateSeq(SeqOf("a", "b", "a")).Last(func(p adt.Indexed[string]) bool { return p.Value == "a" })
	if idx.IsNone() || idx.MustGet().Index != 2 {
		t.Errorf("Last() = %v, want the match at index 2", idx)
	}
	if SeqOf(1, 2, 3).Last(func(n int) bool { return n == 99 }).IsSome() {
		t.Error("Last() found a match that doesn't exist")
	}
}

func TestSeq_GroupBy(t *testing.T) {
	got := SeqOf(1, 2, 3, 4, 5, 6).GroupBy(func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	})
	want := map[string][]int{"odd": {1, 3, 5}, "even": {2, 4, 6}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GroupBy() = %v, want %v", got, want)
	}
}

func TestSeq_Partition(t *testing.T) {
	even, odd := SeqOf(1, 2, 3, 4, 5, 6).Partition(func(n int) bool { return n%2 == 0 })
	if want := []int{2, 4, 6}; !reflect.DeepEqual(even, want) {
		t.Errorf("Partition() matched = %v, want %v", even, want)
	}
	if want := []int{1, 3, 5}; !reflect.DeepEqual(odd, want) {
		t.Errorf("Partition() unmatched = %v, want %v", odd, want)
	}
}

func TestSeq_Nth(t *testing.T) {
	if got := SeqOf("a", "b", "c", "d").Nth(2); got.IsNone() || got.MustGet() != "c" {
		t.Errorf("Nth(2) = %v, want Some(c)", got)
	}
	if got := SeqOf("a", "b").Nth(0); got.IsNone() || got.MustGet() != "a" {
		t.Errorf("Nth(0) = %v, want Some(a)", got)
	}
	if SeqOf(1, 2).Nth(5).IsSome() {
		t.Error("Nth past the end should be None")
	}
	if SeqOf(1, 2).Nth(-1).IsSome() {
		t.Error("Nth(-1) should be None")
	}
}

func TestSeq_Nth_StopsAfterNthElement(t *testing.T) {
	pulled := 0
	naturals := FromSeq(func(yield func(int) bool) {
		for i := 0; ; i++ {
			pulled++
			if !yield(i) {
				return
			}
		}
	})
	if got := naturals.Nth(3); got.IsNone() || got.MustGet() != 3 {
		t.Fatalf("Nth(3) = %v, want Some(3)", got)
	}
	if pulled != 4 {
		t.Errorf("Nth(3) pulled %d elements, want 4", pulled)
	}
}

func TestRange(t *testing.T) {
	if got, want := Range(0, 5).Collect(), []int{0, 1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("Range(0, 5) = %v, want %v", got, want)
	}
	if got := Range(5, 5).Collect(); len(got) != 0 {
		t.Errorf("Range(5, 5) = %v, want empty", got)
	}
	if got := Range(5, 2).Collect(); len(got) != 0 {
		t.Errorf("Range(5, 2) = %v, want empty", got)
	}
	if got, want := Range(0.0, 2.5).Collect(), []float64{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("Range(0.0, 2.5) = %v, want %v", got, want)
	}
	if got, want := Range[uint8](253, 255).Collect(), []uint8{253, 254}; !reflect.DeepEqual(got, want) {
		t.Errorf("Range[uint8](253, 255) = %v, want %v", got, want)
	}
}

func TestRepeat(t *testing.T) {
	if got, want := Repeat("x").Take(3).Collect(), []string{"x", "x", "x"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Repeat(x).Take(3) = %v, want %v", got, want)
	}
}

func TestIterate(t *testing.T) {
	calls := 0
	got := Iterate(1, func(n int) int { calls++; return n * 2 }).Take(5).Collect()
	if want := []int{1, 2, 4, 8, 16}; !reflect.DeepEqual(got, want) {
		t.Errorf("Iterate(1, *2).Take(5) = %v, want %v", got, want)
	}
	if calls != 4 {
		t.Errorf("Iterate called next %d times for 5 elements, want 4", calls)
	}
}

func TestGenerate(t *testing.T) {
	calls := 0
	got := Generate(func() int { calls++; return calls }).Take(3).Collect()
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("Generate().Take(3) = %v, want %v", got, want)
	}
	if calls != 3 {
		t.Errorf("Generate called fn %d times for 3 elements, want 3", calls)
	}
}

func TestUnfold_Infinite(t *testing.T) {
	fib := Unfold(adt.PairOf(0, 1), func(p adt.Pair[int, int]) adt.Option[adt.Pair[int, adt.Pair[int, int]]] {
		a, b := p.Unpack()
		return adt.Some(adt.PairOf(a, adt.PairOf(b, a+b)))
	})
	if got, want := fib.Take(7).Collect(), []int{0, 1, 1, 2, 3, 5, 8}; !reflect.DeepEqual(got, want) {
		t.Errorf("fib.Take(7) = %v, want %v", got, want)
	}
}

func TestUnfold_EndsOnNone(t *testing.T) {
	calls := 0
	countdown := Unfold(3, func(n int) adt.Option[adt.Pair[string, int]] {
		calls++
		if n == 0 {
			return adt.None[adt.Pair[string, int]]()
		}
		return adt.Some(adt.PairOf(strconv.Itoa(n), n-1))
	})
	if got, want := countdown.Collect(), []string{"3", "2", "1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("countdown = %v, want %v", got, want)
	}
	if calls != 4 {
		t.Errorf("Unfold called step %d times, want 4", calls)
	}
}

func TestUnfold_IsLazy(t *testing.T) {
	calls := 0
	Unfold(0, func(n int) adt.Option[adt.Pair[int, int]] {
		calls++
		return adt.Some(adt.PairOf(n, n+1))
	})
	if calls != 0 {
		t.Errorf("Unfold called step %d times before iteration, want 0", calls)
	}
}

func TestFromSeq2(t *testing.T) {
	got := FromSeq2(slices.All([]string{"a", "b", "c"})).Collect()
	want := []adt.Pair[int, string]{adt.PairOf(0, "a"), adt.PairOf(1, "b"), adt.PairOf(2, "c")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FromSeq2(slices.All) = %v, want %v", got, want)
	}
}

func TestFromSeq2_StopsSourceEarly(t *testing.T) {
	pulled := 0
	src := func(yield func(int, int) bool) {
		for i := 0; ; i++ {
			pulled++
			if !yield(i, i*i) {
				return
			}
		}
	}
	if got := FromSeq2(src).Take(2).Count(); got != 2 {
		t.Fatalf("Take(2).Count() = %d, want 2", got)
	}
	if pulled != 2 {
		t.Errorf("FromSeq2 pulled %d pairs for Take(2), want 2", pulled)
	}
}

func TestToSeq2(t *testing.T) {
	s := SeqOf(adt.PairOf("x", 1), adt.PairOf("y", 2))
	var keys []string
	var vals []int
	for k, v := range ToSeq2(s) {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	if want := []string{"x", "y"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("ToSeq2 keys = %v, want %v", keys, want)
	}
	if want := []int{1, 2}; !reflect.DeepEqual(vals, want) {
		t.Errorf("ToSeq2 values = %v, want %v", vals, want)
	}
}

func TestToSeq2_StopsWhenConsumerBreaks(t *testing.T) {
	var keys []int
	for k := range ToSeq2(FromSeq2(slices.All([]string{"a", "b", "c"}))) {
		keys = append(keys, k)
		if k == 1 {
			break
		}
	}
	if want := []int{0, 1}; !reflect.DeepEqual(keys, want) {
		t.Errorf("ToSeq2 with break = %v, want %v", keys, want)
	}
}

func TestSeq2_RoundTripThroughMap(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	got := maps.Collect(ToSeq2(FromSeq2(maps.All(m))))
	if !reflect.DeepEqual(got, m) {
		t.Errorf("round trip = %v, want %v", got, m)
	}
}
