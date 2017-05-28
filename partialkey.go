// Package partialkey implements Partial Key Grouping
/*
   Partial Key Grouping: Load-Balanced Partitioning of Distributed Streams
   https://arxiv.org/abs/1510.07623

*/
package partialkey

type NodeID int

type Choose func(key string) NodeID

type Grouper struct {
	loads map[NodeID]int
	c1    Choose
	c2    Choose
}

func New(c1, c2 Choose) *Grouper {
	return &Grouper{
		loads: make(map[NodeID]int),
		c1:    c1,
		c2:    c2,
	}
}

func (g *Grouper) NodeForKey(key string) NodeID {
	first := g.c1(key)
	second := g.c2(key)

	selected := first
	if g.loads[first] > g.loads[second] {
		selected = second
	}

	g.loads[selected]++

	return selected
}
