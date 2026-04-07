package sharding

import "testing"

func TestRingReturnsStableShardForSameKey(t *testing.T) {
	ring := NewRing([]string{"shard-a", "shard-b", "shard-c"}, 10)

	first := ring.ShardFor("pet")
	second := ring.ShardFor("pet")

	if first == "" {
		t.Fatalf("expected a shard assignment")
	}
	if first != second {
		t.Fatalf("expected stable shard mapping, got %q and %q", first, second)
	}
}

func TestRingWrapsAroundWhenHashIsPastLastPoint(t *testing.T) {
	ring := NewRing([]string{"shard-a"}, 1)

	if got := ring.ShardFor("anything"); got != "shard-a" {
		t.Fatalf("expected shard-a, got %q", got)
	}
}
