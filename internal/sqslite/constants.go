package sqslite

import "time"

const (
	// DefaultQueueName is the name of a default queue created automatically.
	DefaultQueueName = "default"
	// DefaultDLQQueueName is the name of a default dlq created automatically.
	DefaultDLQQueueName = "default-dlq"

	DefaultHost      = "sqslite.local"
	DefaultRegion    = "us-west-2"
	DefaultAccountID = "sqslite-test-account"

	DefaultQueueMaximumMessageSizeBytes = 256 * 1024         // 256KiB
	DefaultQueueMessageRetentionPeriod  = 4 * 24 * time.Hour // 4 days
	DefaultQueueReceiveMessageWaitTime  = 20 * time.Second
	DefaultQueueVisibilityTimeout       = 30 * time.Second
)

const (
	// HeaderAmzTarget is the header used to indicate what rpc method we're invoking.
	HeaderAmzTarget = "X-Amz-Target"
	// HeaderAmzQueryMode it is unclear to me what this does yet.
	HeaderAmzQueryMode = "X-Amzn-Query-Mode"
	// HeaderAmznRequestID is used to report back a request id to the client.
	HeaderAmznRequestID = "X-Amzn-Requestid"
	// ContentTypeAmzJSON is the amz-json-1.0 content type header value.
	ContentTypeAmzJSON = "application/x-amz-json-1.0"
)

// Method names
const (
	MethodCreateQueue                  = "AmazonSQS.CreateQueue"
	MethodListQueues                   = "AmazonSQS.ListQueues"
	MethodListDeadLetterSourceQueues   = "AmazonSQS.ListDeadLetterSourceQueues"
	MethodGetQueueAttributes           = "AmazonSQS.GetQueueAttributes"
	MethodSetQueueAttributes           = "AmazonSQS.SetQueueAttributes"
	MethodListQueueTags                = "AmazonSQS.ListQueueTags"
	MethodTagQueue                     = "AmazonSQS.TagQueue"
	MethodUntagQueue                   = "AmazonSQS.UntagQueue"
	MethodPurgeQueue                   = "AmazonSQS.PurgeQueue"
	MethodDeleteQueue                  = "AmazonSQS.DeleteQueue"
	MethodReceiveMessage               = "AmazonSQS.ReceiveMessage"
	MethodSendMessage                  = "AmazonSQS.SendMessage"
	MethodDeleteMessage                = "AmazonSQS.DeleteMessage"
	MethodChangeMessageVisibility      = "AmazonSQS.ChangeMessageVisibility"
	MethodSendMessageBatch             = "AmazonSQS.SendMessageBatch"
	MethodDeleteMessageBatch           = "AmazonSQS.DeleteMessageBatch"
	MethodChangeMessageVisibilityBatch = "AmazonSQS.ChangeMessageVisibilityBatch"
	MethodStartMessageMoveTask         = "AmazonSQS.StartMessageMoveTask"
	MethodCancelMessageMoveTask        = "AmazonSQS.CancelMessageMoveTask"
	MethodListMessageMoveTasks         = "AmazonSQS.ListMessageMoveTasks"

	/* left to implement */
	MethodAddPermission    = "AmazonSQS.AddPermission"
	MethodRemovePermission = "AmazonSQS.RemovePermission"
)

const (
	AttributeTypeString = "String"
	AttributeTypeNumber = "Number"
	AttributeTypeBinary = "Binary"
)

const (
	MessageAttributeApproximateReceiveCount          = "ApproximateReceiveCount"
	MessageAttributeApproximateFirstReceiveTimestamp = "ApproximateFirstReceiveTimestamp"
	MessageAttributeSenderID                         = "SenderId"
	MessageAttributeSentTimestamp                    = "SentTimestamp"
	MessageAttributeAWSTraceHeader                   = "AWSTraceHeader"
	MessageAttributeDeadLetterQueueSourceArn         = "DeadLetterQueueSourceArn"

	/* not implementing fifo queues, so these are not needed */
	MessageAttributeSequenceNumber         = "SequenceNumber"
	MessageAttributeMessageGroupID         = "MessageGroupId"
	MessageAttributeMessageDeduplicationId = "MessageDeduplicationId"
)
