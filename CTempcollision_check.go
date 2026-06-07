package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

func hashVNode(domain string, seed uint32) uint32 {
	h := fnv.New32a()
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], seed)
	h.Write(buf[:])
	h.Write([]byte(domain))
	return h.Sum32()
}

func main() {
	nodes := []string{"n1", "n2", "n3"}
	vnodes := 128
	positions := make(map[uint32]string)
	collisions := 0
	for _, n := range nodes {
		for i := 0; i < vnodes; i++ {
			p := hashVNode(n, uint32(i))
			if owner, exists := positions[p]; exists {
				fmt.Printf("COLLISION: pos=%d claimed by %s and %s\n", p, owner, n)
				collisions++
			} else {
				positions[p] = n
			}
		}
	}
	fmt.Printf("Total unique positions: %d / %d expected, collisions: %d\n", len(positions), len(nodes)*vnodes, collisions)
}
