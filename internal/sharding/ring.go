package sharding

import (
	"hash/fnv"
	"sort"
)

// Ring maps keys to shards using consistent hashing with virtual nodes.
type Ring struct {
	points []point
}

type point struct {
	hash  uint32
	shard string
}

// NewRing creates a ring from shard names and a virtual-node count.
func NewRing(shards []string, virtualNodes int) *Ring {
	points := make([]point, 0, len(shards)*virtualNodes)
	for _, shard := range shards {
		for i := 0; i < virtualNodes; i++ {
			points = append(points, point{
				hash:  hashString(shard + "#" + itoa(i)),
				shard: shard,
			})
		}
	}

	sort.Slice(points, func(i, j int) bool {
		if points[i].hash == points[j].hash {
			return points[i].shard < points[j].shard
		}
		return points[i].hash < points[j].hash
	})

	return &Ring{points: points}
}

// ShardFor returns the shard responsible for key.
func (r *Ring) ShardFor(key string) string {
	if len(r.points) == 0 {
		return ""
	}

	hash := hashString(key)
	index := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= hash
	})
	if index == len(r.points) {
		index = 0
	}
	return r.points[index].shard
}

func hashString(value string) uint32 {
	hasher := fnv.New32a()
	if _, err := hasher.Write([]byte(value)); err != nil {
		return 0
	}
	return hasher.Sum32()
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}

	buf := [20]byte{}
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + (v % 10))
		v /= 10
	}
	return string(buf[pos:])
}
