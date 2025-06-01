package sqslite

import (
	"context"
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

func (a *Accounts) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, queues := range a.accounts {
		queues.Close()
	}
}
