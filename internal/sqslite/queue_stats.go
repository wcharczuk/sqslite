package sqslite

// Stats are basic statistics about the queue.
type QueueStats struct {
	NumMessages                    int64
	NumMessagesReady               int64
	NumMessagesDelayed             int64
	NumMessagesInflight            int64
	TotalMessagesSent              uint64
	TotalMessagesReceived          uint64
	TotalMessagesMoved             uint64
	TotalMessagesDeleted           uint64
	TotalMessagesChangedVisibility uint64
	TotalMessagesPurged            uint64
	TotalMessagesInflightToReady   uint64
	TotalMessagesDelayedToReady    uint64
	TotalMessagesInflightToDLQ     uint64
}
