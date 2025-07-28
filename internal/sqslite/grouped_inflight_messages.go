package sqslite

import (
	"iter"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

func newGroupedInflightMessages() *groupedInflightMessages {
	return &groupedInflightMessages{
		receiptHandles: make(map[string]*MessageState),
		groups:         make(map[string]map[uuid.UUID]*MessageState),
	}
}

type groupedInflightMessages struct {
	receiptHandles map[string]*MessageState
	groups         map[string]map[uuid.UUID]*MessageState
	len            int
}

func (i *groupedInflightMessages) Len() int {
	return i.len
}

func (i *groupedInflightMessages) ValidGroups(maximumLength int) (output []string) {
	output = make([]string, 0, len(i.groups))
	for group, list := range i.groups {
		if len(list) < maximumLength {
			output = append(output, group)
		}
	}
	return
}

func (i *groupedInflightMessages) Push(receiptHandle string, msg *MessageState) {
	if _, ok := i.groups[msg.MessageGroupID]; !ok {
		i.groups[msg.MessageGroupID] = make(map[uuid.UUID]*MessageState)
	}
	i.groups[msg.MessageGroupID][msg.MessageID] = msg
	i.receiptHandles[receiptHandle] = msg
	i.len++
}

func (i *groupedInflightMessages) Get(group string, id uuid.UUID) (msg *MessageState, ok bool) {
	list, hasList := i.groups[group]
	if !hasList || len(list) == 0 {
		return
	}
	msg, ok = list[id]
	return
}

func (i *groupedInflightMessages) GetByReceiptHandle(receiptHandle string) (msg *MessageState, ok bool) {
	msg, ok = i.receiptHandles[receiptHandle]
	return
}

func (i *groupedInflightMessages) RemoveByReceiptHandle(receiptHandle string) (ok bool) {
	var msg *MessageState
	msg, ok = i.receiptHandles[receiptHandle]
	if !ok {
		return
	}
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(i.receiptHandles, receiptHandle)
	}
	delete(i.receiptHandles, receiptHandle)
	list, hasList := i.groups[msg.MessageGroupID]
	if !hasList || len(list) == 0 {
		return
	}
	delete(list, msg.MessageID)
	i.len--
	return
}

func (i *groupedInflightMessages) Remove(msg *MessageState) {
	for receiptHandle := range msg.ReceiptHandles.Consume() {
		delete(i.receiptHandles, receiptHandle)
	}
	list, ok := i.groups[msg.MessageGroupID]
	if !ok || len(list) == 0 {
		return
	}
	delete(list, msg.MessageID)
	i.len--
}

func (i *groupedInflightMessages) Each() iter.Seq[*MessageState] {
	return func(yield func(*MessageState) bool) {
		for _, group := range i.groups {
			for _, msg := range group {
				if !yield(msg) {
					return
				}
			}
		}
	}
}
