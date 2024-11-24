package sqslite

import "time"

// Message is a message in the queue.
type Message struct {
	QueueURL          string `json:"queue_url"`
	MessageBody       []byte
	MessageAttributes map[string]AttributeValue
	Delay             time.Duration
}

// AttributeValue is a piece of metadata.
type AttributeValue struct {
	DataType    string
	StringValue string
}
