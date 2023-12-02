package mclock

import (
	"container/list"
)

type Slo struct {
	Reserve uint64
	Weight  float64
	Limit   uint64
}

type MClock struct {
	systemThroughput uint64
	throughputProp   uint64
	clients          map[int]*list.List
	tags             []*sloTag
	virtualClock     uint64
	minTagR          deadline
	minTagP          deadline
	size             uint64
}

func NewMClock(systemThroughput uint64) *MClock {
	return &MClock{
		systemThroughput: systemThroughput,
		clients:          make(map[int]*list.List),
		virtualClock:     1,
	}
}

func (mc *MClock) Enqueue(clientId int, slo Slo, req interface{}) {
	cli, ok := mc.clients[clientId]
	if !ok {
		mc.newClient(clientId, slo)
		cli = list.New()
		mc.clients[clientId] = cli
	} else {
		if cli.Len() == 0 {
			idx := mc.getClientIdx(clientId)
			mc.activeTag(idx)
		}
	}
	cli.PushBack(req)
	mc.size++
}

func (mc *MClock) Dequeue() interface{} {
	tag, idx := mc.front()
	for mc.size > 0 && tag == nil {
		mc.virtualClock++
		mc.calMinTags()
	}
	cli := mc.clients[tag.clientId]
	req := cli.Front()
	cli.Remove(req)
	if cli.Len() == 0 {
		mc.tags[idx].active = false
	}
	mc.virtualClock++
	mc.advanceDeadline(idx)
	mc.calMinTags()
	mc.size--
	return req.Value
}

func (mc *MClock) front() (*sloTag, int) {
	now := mc.currentClock()
	if mc.minTagR.valid {
		tag := mc.tags[mc.minTagR.cliIdx]
		if tag.deadlineR <= float64(now) {
			tag.selTag = 1
			return tag, mc.minTagR.cliIdx
		}
	}
	if mc.minTagP.valid {
		tag := mc.tags[mc.minTagP.cliIdx]
		if tag.deadlineP > 0 && tag.deadlineL <= float64(now) {
			tag.selTag = 2
			return tag, mc.minTagP.cliIdx
		}
	}
	return nil, 0
}

func (mc *MClock) advanceDeadline(idx int) {
	tag := mc.tags[idx]
	if tag.selTag == 1 {
		if tag.deadlineR > 0 {
			tag.deadlineR += tag.spacingR
		}
	}
	if tag.deadlineP > 0 {
		tag.deadlineP += tag.spacingP
	}
	if tag.deadlineL > 0 {
		tag.deadlineL += tag.spacingL
	}
}

func (mc *MClock) newClient(clientId int, slo Slo) {
	if slo.Limit < slo.Reserve {
		slo.Limit = slo.Reserve
	}

	tag := &sloTag{clientId: clientId, slo: slo, active: true}
	if slo.Reserve > 0 {
		tag.deadlineR = float64(mc.currentClock())
		tag.spacingR = float64(mc.systemThroughput) / float64(slo.Reserve)
	}
	if slo.Limit > 0 {
		tag.deadlineL = float64(mc.currentClock())
		tag.spacingL = float64(mc.systemThroughput) / float64(slo.Limit)
	}
	if slo.Weight > 0 {
		mc.throughputProp += uint64(slo.Weight)
	}
	mc.tags = append(mc.tags, tag)

	for _, tt := range mc.tags {
		tt.deadlineP = float64(mc.currentClock())
		tt.spacingP = float64(mc.throughputProp) / tt.slo.Weight
	}

	mc.calMinTags()
}

func (mc *MClock) currentClock() uint64 {
	return mc.virtualClock
}

func (mc *MClock) calMinTags() {
	mc.minTagR.valid = false
	mc.minTagP.valid = false
	for idx, tag := range mc.tags {
		if !tag.active {
			continue
		}

		if tag.deadlineR > 0 && (tag.deadlineL <= float64(mc.currentClock())) {
			if mc.minTagR.valid {
				if mc.minTagR.dl >= tag.deadlineR {
					mc.minTagR.cliIdx = idx
					mc.minTagR.dl = tag.deadlineR
				}
			} else {
				mc.minTagR.cliIdx = idx
				mc.minTagR.dl = tag.deadlineR
				mc.minTagR.valid = true
			}
		}

		if tag.deadlineP > 0 && (tag.deadlineL <= float64(mc.currentClock())) {
			if mc.minTagP.valid {
				if mc.minTagP.dl >= tag.deadlineP {
					mc.minTagP.cliIdx = idx
					mc.minTagP.dl = tag.deadlineP
				}
			} else {
				mc.minTagP.cliIdx = idx
				mc.minTagP.dl = tag.deadlineP
				mc.minTagP.valid = true
			}
		}
	}
}

func (mc *MClock) getClientIdx(clientId int) int {
	for i, tag := range mc.tags {
		if tag.clientId == clientId {
			return i
		}
	}
	panic("clientId does not exist")
}

func (mc *MClock) activeTag(idx int) {
	now := mc.currentClock()
	tag := mc.tags[idx]
	tag.active = true
	if tag.deadlineR > 0 {
		tag.deadlineR = max((tag.deadlineR + tag.spacingR), float64(now))
	}
	if tag.deadlineP > 0 {
		tag.deadlineP = mc.minTagP.dl
	}
	if tag.deadlineL > 0 {
		tag.deadlineL = max(tag.deadlineL+tag.spacingL, float64(now))
	}
	mc.calMinTags()
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

type sloTag struct {
	deadlineR float64
	spacingR  float64
	deadlineL float64
	spacingL  float64
	deadlineP float64
	spacingP  float64
	active    bool
	selTag    int
	clientId  int
	slo       Slo
}

type deadline struct {
	cliIdx int
	dl     float64
	valid  bool
}
