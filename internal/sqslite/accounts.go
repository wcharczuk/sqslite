package sqslite

import (
	"sync"
)

// NewAccounts returns a new accounts set.
func NewAccounts() *Accounts {
	return &Accounts{
		accounts: map[string]*Queues{
			DefaultAccountID: NewQueues(),
		},
	}
}

type Accounts struct {
	mu       sync.Mutex
	accounts map[string]*Queues
}

func (a *Accounts) EnsureQueues(accountID string) *Queues {
	a.mu.Lock()
	defer a.mu.Unlock()
	if queues, ok := a.accounts[accountID]; ok {
		return queues
	}
	newQueues := NewQueues()
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
