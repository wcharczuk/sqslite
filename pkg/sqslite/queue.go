package sqslite

import (
	"sync"
	"time"

	"sqslite/pkg/uuid"
)

var (
	QueueAttributeDelaySeconds           = "DelaySeconds"
	QueueAttributeMessageRetentionPeriod = "MessageRetentionPeriod"
)

type Queue struct {
	Name              string
	URL               string
	VisibilityTimeout time.Duration
	IsDLQ             bool
	Attributes        map[string]string

	storageMu sync.Mutex
	storage   map[uuid.UUID]*SqsMessage
}
