package server

import (
	"context"
	"sqslite/pkg/api"
	"sqslite/pkg/sqslite"
)

type Server struct {
	Queues *sqslite.Queues
}

func (s *Server) CreateQueue(ctx context.Context, input *api.CreateQueueInput) (*api.CreateQueueOutput, error) {
	return nil, nil
}
