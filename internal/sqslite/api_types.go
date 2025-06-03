package sqslite

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
)

type SendMessageBatchOutput struct {
	// A list of BatchResultErrorEntry items with error details about each message that can't be enqueued.
	//
	// This member is required.
	Failed []BatchResultErrorEntry
	// A list of SendMessageBatchResultEntry items.
	//
	// This member is required.
	Successful []types.SendMessageBatchResultEntry
	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata `json:"ResultMetadata,omitzero"`
}

// BatchErrorEntry is an entry in the [SendMessageBatchOutput.Failed] list.
type BatchResultErrorEntry struct {
	Error
	ID string `json:"Id"`
}
