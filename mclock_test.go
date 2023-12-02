package mclock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMClock(t *testing.T) {

	rdSlo := Slo{Reserve: 250, Weight: 100, Limit: 500000000}
	oltpSlo := Slo{Reserve: 250, Weight: 200, Limit: 500000000}
	dmSlot := Slo{Reserve: 0, Weight: 300, Limit: 1000}

	data := []struct {
		throughput uint64
		rd         uint64
		oltp       uint64
		dm         uint64
	}{{250, 125, 125, 0}, {500, 250, 250, 0}, {700, 250, 250, 200},
		{875, 250, 250, 375}, {1500, 250, 500, 750}, {2000, 333, 667, 1000},
		{2500, 500, 1000, 1000}, {3000, 667, 1333, 1000}}

	for _, da := range data {
		clis := []uint64{0, 0, 0}
		clock := NewMClock(da.throughput)
		for i := uint64(0); i < da.throughput; i++ {
			clock.Enqueue(1, rdSlo, 1)
			clock.Enqueue(2, oltpSlo, 2)
			clock.Enqueue(3, dmSlot, 3)
		}

		for i := uint64(0); i < da.throughput; i++ {
			req := clock.Dequeue().(int)
			clis[req-1]++
		}
		assert.Equal(t, da.rd, clis[0])
		assert.Equal(t, da.oltp, clis[1])
		assert.Equal(t, da.dm, clis[2])
	}
}
