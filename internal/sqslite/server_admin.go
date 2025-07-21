package sqslite

import (
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/wcharczuk/sqslite/internal/httpz"
)

type QueueInfo struct {
	AccountID    string
	QueueURL     string
	QueueArn     string
	QueueName    string
	Created      time.Time
	LastModified time.Time
	Deleted      time.Time
	IsDLQ        bool
	Stats        QueueStats
}

func (s Server) RegisterAdmin() {
	s.router.GET("/admin/accounts", s.adminGetAccounts)
	s.router.GET("/admin/account/:account_id/queues", s.adminGetQueues)
	s.router.GET("/admin/account/:account_id/queue/:queue_name", s.adminGetQueue)
}

func (s Server) adminGetAccounts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	accounts := slices.Collect(s.accounts.EachAccount())
	w.Header().Set(httpz.HeaderContentType, httpz.ContentTypeApplicationJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(accounts)
}

func (s Server) adminGetQueues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	accountID := ps.ByName("account_id")
	if accountID == "" {
		http.NotFound(w, r)
		return
	}
	queues := slices.Collect(s.accounts.EnsureQueues(accountID).EachQueue())
	output := apply(queues, func(q *Queue) QueueInfo {
		return QueueInfo{
			AccountID:    q.AccountID,
			QueueURL:     q.URL,
			QueueArn:     q.ARN,
			QueueName:    q.Name,
			Created:      q.Created(),
			LastModified: q.LastModified(),
			Deleted:      q.Deleted(),
			IsDLQ:        q.IsDLQ(),
			Stats:        q.Stats(),
		}
	})
	w.Header().Set(httpz.HeaderContentType, httpz.ContentTypeApplicationJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(output)
}

func (s Server) adminGetQueue(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	accountID := ps.ByName("account_id")
	if accountID == "" {
		http.NotFound(w, r)
		return
	}
	queueURL, ok := s.accounts.EnsureQueues(accountID).GetQueueURL(ps.ByName("queue_name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	queue, ok := s.accounts.EnsureQueues(accountID).GetQueue(queueURL)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set(httpz.HeaderContentType, httpz.ContentTypeApplicationJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(queue)
}
