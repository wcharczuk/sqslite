package sqslite

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

// ReceiptHandle is eventually turned into
// the resulting handle string for messages returned
// by [Queue.Receive].
type ReceiptHandle struct {
	ID           uuid.UUID
	QueueARN     string
	MessageID    string
	LastReceived time.Time
}

// String returns a string form of the receipt handle.
func (r ReceiptHandle) String() string {
	return base64.StdEncoding.EncodeToString(
		fmt.Appendf(nil, "%s %s %s %s",
			r.ID.String(),
			r.QueueARN,
			r.MessageID,
			r.LastReceived.Format(time.RFC3339),
		),
	)
}
