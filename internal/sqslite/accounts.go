package sqslite

import (
	"context"
	"iter"
	"sync"

	"github.com/jonboulle/clockwork"
)

// NewAccounts returns a new accounts set.
func NewAccounts(clock clockwork.Clock) *Accounts {
	return &Accounts{
		accounts: map[string]*Queues{},
		clock:    clock,
	}
}

type Accounts struct {
	mu       sync.Mutex
	accounts map[string]*Queues
	clock    clockwork.Clock
}

func (a *Accounts) EnsureQueues(accountID string) *Queues {
	a.mu.Lock()
	defer a.mu.Unlock()
	if queues, ok := a.accounts[accountID]; ok {
		return queues
	}
	newQueues := NewQueues(a.clock, accountID)
	newQueues.Start(context.Background())
	a.accounts[accountID] = newQueues
	return newQueues
}

// EachQueue returns an iterator for every queue in the server across all accounts.
func (a *Accounts) EachAccount() iter.Seq[string] {
	return func(yield func(string) bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		for accountID := range a.accounts {
			if !yield(accountID) {
				return
			}
		}
	}
}

// EachQueue returns an iterator for every queue in the server across all accounts.
func (a *Accounts) EachQueue() iter.Seq[*Queue] {
	return func(yield func(*Queue) bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, accountQueues := range a.accounts {
			if !a.eachQueueInAccount(accountQueues, yield) {
				return
			}
		}
	}
}

func (a *Accounts) eachQueueInAccount(queues *Queues, yield func(*Queue) bool) bool {
	queues.queuesMu.Lock()
	defer queues.queuesMu.Unlock()
	for _, queue := range queues.queues {
		if !yield(queue) {
			return false
		}
	}
	return true
}

func (a *Accounts) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, queues := range a.accounts {
		queues.Close()
	}
}
