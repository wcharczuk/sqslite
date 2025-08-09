package sqslite

import (
	"iter"
	"math"
	"slices"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

func newInflightMessages() *inflightMessages {
	return &inflightMessages{
		receiptHandles: make(map[string]*MessageState),
		messages:       make(map[uuid.UUID]*MessageState),
		keyCounts:      make(map[string]int),
	}
}

type inflightMessages struct {
	receiptHandles map[string]*MessageState
	messages       map[uuid.UUID]*MessageState
	keyCounts      map[string]int
	len            int
}

func (i *inflightMessages) Len() int {
	return i.len
}

func (i *inflightMessages) HotKeys() (hotKeys Set[string]) {
	hotKeys = make(Set[string])
	type keyCount struct {
		key   string
		count int
	}
	keyCounts := make([]keyCount, 0, len(i.keyCounts))
	for key, count := range i.keyCounts {
		keyCounts = append(keyCounts, keyCount{key, count})
	}
	slices.SortFunc(keyCounts, func(i, j keyCount) int {
		if i.count > j.count {
			return -1
		} else if i.count < j.count {
			return 1
		}
		return 0
	})
	calculateCountMean := func() float64 {
		accum := uint64(0)
		for _, kc := range keyCounts {
			accum += uint64(kc.count)
		}
		return float64(accum) / float64(len(keyCounts))
	}
	mean := calculateCountMean()
	caclulateCountStdDev := func() float64 {
		var variance float64
		for _, kc := range keyCounts {
			variance += ((float64(kc.count) - mean) * (float64(kc.count) - mean))
		}
		variance = variance / float64(len(keyCounts))
		return math.Sqrt(variance)
	}
	stdDev := caclulateCountStdDev()

	// don't bother if we don't have a significant stddev
	if stdDev < 1.0 {
		return
	}

	// the threshold here will be > stddev above the mean count with a maximum hot keys of 10
	for _, kc := range keyCounts {
		if float64(kc.count) > (mean + stdDev) {
			hotKeys.Add(kc.key)
			if len(hotKeys) == 10 {
				return
			}
		}
	}
	return
}

func (i *inflightMessages) Push(receiptHandle string, msg *MessageState) {
	i.messages[msg.MessageID] = msg
	i.receiptHandles[receiptHandle] = msg
	if messageGroupID := msg.MessageGroupID; messageGroupID != "" {
		i.keyCounts[messageGroupID]++
	}
	i.len++
}

func (i *inflightMessages) Get(id uuid.UUID) (msg *MessageState, ok bool) {
	msg, ok = i.messages[id]
	return
}

func (i *inflightMessages) GetByReceiptHandle(receiptHandle string) (msg *MessageState, ok bool) {
	msg, ok = i.receiptHandles[receiptHandle]
	return
}

func (i *inflightMessages) RemoveByReceiptHandle(receiptHandle string) (ok bool) {
	var msg *MessageState
	msg, ok = i.receiptHandles[receiptHandle]
	if !ok {
		return
	}
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(i.receiptHandles, receiptHandle)
	}
	delete(i.receiptHandles, receiptHandle)
	delete(i.messages, msg.MessageID)
	i.keyCounts[msg.MessageGroupID]--
	i.len--
	return
}

func (i *inflightMessages) Remove(msg *MessageState) {
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(i.receiptHandles, receiptHandle)
	}
	delete(i.messages, msg.MessageID)
	i.keyCounts[msg.MessageGroupID]--
	i.len--
}

func (i *inflightMessages) Each() iter.Seq[*MessageState] {
	return func(yield func(*MessageState) bool) {
		for _, msg := range i.messages {
			if !yield(msg) {
				return
			}
		}
	}
}
