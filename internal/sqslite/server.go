package sqslite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/jonboulle/clockwork"
	"github.com/julienschmidt/httprouter"

	"github.com/wcharczuk/sqslite/internal/uuid"
)

// NewServer returns a new server.
func NewServer(clock clockwork.Clock) *Server {
	server := &Server{
		accounts: NewAccounts(clock),
		router:   httprouter.New(),
		clock:    clock,
	}

	server.RegisterAdmin()
	return server
}

var _ http.Handler = (*Server)(nil)

// Server implements the http routing layer for sqslite.
type Server struct {
	accounts *Accounts
	router   *httprouter.Router
	clock    clockwork.Clock
}

// Clock returns the server's [clockwork.Clock] instance.
func (s *Server) Clock() clockwork.Clock {
	return s.clock
}

// Queues returns the underlying queues storage.
func (s *Server) Accounts() *Accounts {
	return s.accounts
}

// Close shuts down the server.
func (s *Server) Close() {
	s.accounts.Close()
}

// ServeHTTP implements [http.Handler].
func (s Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, "/admin/") {
		s.router.ServeHTTP(rw, req)
		return
	}

	if req.Method != http.MethodPost {
		s.unknownMethod(rw, req)
		return
	}
	if req.URL.Path != "/" {
		s.unknownPath(rw, req)
		return
	}

	authz, err := getRequestAuthorization(req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	req = req.WithContext(
		WithContextAuthorization(req.Context(), authz),
	)

	action := req.Header.Get(HeaderAmzTarget)
	switch action {
	case MethodCreateQueue:
		s.createQueue(rw, req)
	case MethodListQueues:
		s.listQueues(rw, req)
	case MethodListDeadLetterSourceQueues:
		s.listDeadLetterSourceQueues(rw, req)
	case MethodGetQueueURL:
		s.getQueueURL(rw, req)
	case MethodGetQueueAttributes:
		s.getQueueAttributes(rw, req)
	case MethodSetQueueAttributes:
		s.setQueueAttributes(rw, req)
	case MethodTagQueue:
		s.tagQueue(rw, req)
	case MethodListQueueTags:
		s.listQueueTags(rw, req)
	case MethodUntagQueue:
		s.untagQueue(rw, req)
	case MethodPurgeQueue:
		s.purgeQueue(rw, req)
	case MethodDeleteQueue:
		s.deleteQueue(rw, req)
	case MethodReceiveMessage:
		s.receiveMessage(rw, req)
	case MethodSendMessage:
		s.sendMessage(rw, req)
	case MethodSendMessageBatch:
		s.sendMessageBatch(rw, req)
	case MethodDeleteMessage:
		s.deleteMessage(rw, req)
	case MethodDeleteMessageBatch:
		s.deleteMessageBatch(rw, req)
	case MethodChangeMessageVisibility:
		s.changeMessageVisibility(rw, req)
	case MethodChangeMessageVisibilityBatch:
		s.changeMessageVisibilityBatch(rw, req)
	case MethodStartMessageMoveTask:
		s.startMessageMoveTask(rw, req)
	case MethodCancelMessageMoveTask:
		s.cancelMoveMessageTask(rw, req)
	case MethodListMessageMoveTasks:
		s.listMoveMessageTasks(rw, req)

	/* these don't really do anything */
	case MethodAddPermission:
		s.addPermission(rw, req)
	case MethodRemovePermission:
		s.removePermission(rw, req)

	default:
		s.invalidMethod(rw, req, action)
	}
}

func (s Server) createQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.CreateQueueInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, err := NewQueueFromCreateQueueInput(s.clock, authz, input)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	err = s.accounts.EnsureQueues(authz.AccountID).AddQueue(queue)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	// we use the background context here because this
	// runs at the scope of the server itself (not the request)
	queue.Start(context.Background())
	serialize(rw, req, &sqs.CreateQueueOutput{
		QueueUrl: &queue.URL,
	})
}

func (s Server) listQueues(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ListQueuesInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	if input.MaxResults != nil && (*input.MaxResults < 0 || *input.MaxResults > 1000) {
		serialize(rw, req, ErrorInvalidAttributeValue().WithMessagef("MaxResults must be greater than 0 and less than 1000, you put %d", *input.MaxResults))
	}
	if input.NextToken != nil && input.MaxResults == nil {
		serialize(rw, req, ErrorInvalidAttributeValue().WithMessagef("MaxResults must be set if NextToken is set"))
	}

	queues := s.accounts.EnsureQueues(authz.AccountID)

	var inputNextToken nextPageToken
	if inputNextTokenRaw := safeDeref(input.NextToken); inputNextTokenRaw != "" {
		inputNextToken = parseNextPageToken(inputNextTokenRaw)
	}

	var maxResults = 1000
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	var nextToken *string
	var queueURLs []string
	var index int
	for q := range queues.EachQueue() {
		if input.QueueNamePrefix != nil && *input.QueueNamePrefix != "" {
			if !strings.HasPrefix(q.Name, *input.QueueNamePrefix) {
				continue
			}
		}
		if inputNextToken.Offset > 0 && inputNextToken.Offset > index {
			index++
			continue
		}
		index++
		queueURLs = append(queueURLs, q.URL)
		if len(queueURLs) == maxResults {
			nextToken = aws.String(nextPageToken{Offset: index}.String())
			break
		}
	}
	serialize(rw, req, &sqs.ListQueuesOutput{
		NextToken: nextToken,
		QueueUrls: queueURLs,
	})
}

func (s Server) listDeadLetterSourceQueues(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ListDeadLetterSourceQueuesInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	if input.MaxResults != nil && (*input.MaxResults < 0 || *input.MaxResults > 1000) {
		serialize(rw, req, ErrorInvalidAttributeValue().WithMessagef("MaxResults must be greater than 0 and less than 1000, you put %d", *input.MaxResults))
		return
	}
	if input.NextToken != nil && input.MaxResults == nil {
		serialize(rw, req, ErrorInvalidAttributeValue().WithMessagef("MaxResults must be set if NextToken is set"))
		return
	}

	queues := s.accounts.EnsureQueues(authz.AccountID)

	var inputNextToken nextPageToken
	if input.NextToken != nil && *input.NextToken != "" {
		inputNextToken = parseNextPageToken(*input.NextToken)
	}

	var maxResults = 1000
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	var nextToken *string
	var queueURLs []string
	var index int
	for q := range queues.EachQueue() {
		if q.dlqTarget == nil {
			continue
		}
		if inputNextToken.Offset > 0 && inputNextToken.Offset > index {
			index++
			continue
		}
		index++
		queueURLs = append(queueURLs, q.URL)
		if len(queueURLs) == maxResults {
			nextToken = aws.String(nextPageToken{Offset: index}.String())
			break
		}
	}
	serialize(rw, req, &sqs.ListDeadLetterSourceQueuesOutput{
		QueueUrls: queueURLs,
		NextToken: nextToken,
	})
}

func (s Server) getQueueURL(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.GetQueueUrlInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}

	accountID := authz.AccountID
	if ownerAccountID := safeDeref(input.QueueOwnerAWSAccountId); ownerAccountID != "" {
		accountID = ownerAccountID
	}

	queueURL, ok := s.accounts.EnsureQueues(accountID).GetQueueURL(safeDeref(input.QueueName))
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	serialize(rw, req, &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(queueURL),
	})
}

func (s Server) getQueueAttributes(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.GetQueueAttributesInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	serialize(rw, req, &sqs.GetQueueAttributesOutput{
		Attributes: queue.GetQueueAttributes(input.AttributeNames...),
	})
}

func (s Server) setQueueAttributes(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SetQueueAttributesInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	if err = queue.SetQueueAttributes(input.Attributes); err != nil {
		serialize(rw, req, err)
		return
	}
	serialize(rw, req, &sqs.SetQueueAttributesOutput{
		ResultMetadata: middleware.Metadata{},
	})
}

func (s Server) listQueueTags(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ListQueueTagsInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	serialize(rw, req, &sqs.ListQueueTagsOutput{
		Tags: queue.Tags,
	})
}

func (s Server) tagQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.TagQueueInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	queue.Tag(input.Tags)
	serialize(rw, req, &sqs.TagQueueOutput{})
}

func (s Server) untagQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.UntagQueueInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	queue.Untag(input.TagKeys)
	serialize(rw, req, &sqs.UntagQueueOutput{})
}

func (s Server) purgeQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.PurgeQueueInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	ok = s.accounts.EnsureQueues(authz.AccountID).PurgeQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	serialize(rw, req, &sqs.PurgeQueueOutput{})
}

func (s Server) deleteQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteQueueInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	ok = s.accounts.EnsureQueues(authz.AccountID).DeleteQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	serialize(rw, req, &sqs.DeleteQueueOutput{})
}

func (s Server) receiveMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ReceiveMessageInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	visibilityTimeout := time.Duration(input.VisibilityTimeout) * time.Second
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		serialize(rw, req, err)
		return
	}
	waitTimeout := time.Duration(input.WaitTimeSeconds) * time.Second
	if err := validateWaitTimeSeconds(waitTimeout); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	allMessages := queue.Receive(input)
	if len(allMessages) > 0 {
		serialize(rw, req, &sqs.ReceiveMessageOutput{
			Messages: allMessages,
		})
		return
	}
	waitTime := coalesceZero(waitTimeout, queue.ReceiveMessageWaitTime)
	ticker := s.clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	waitDeadline := s.clock.NewTimer(waitTime)
	defer waitDeadline.Stop()

done:
	for {
		select {
		case <-req.Context().Done():
			return
		case <-waitDeadline.Chan():
			break done
		case <-ticker.Chan():
			allMessages = queue.Receive(input)
			if len(allMessages) > 0 {
				break done
			}
		}
	}
	serialize(rw, req, &sqs.ReceiveMessageOutput{
		Messages: allMessages,
	})
}

func (s Server) sendMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SendMessageInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	if err := validateMessageBody(input.MessageBody, queue.MaximumMessageSizeBytes); err != nil {
		serialize(rw, req, err)
		return
	}
	if err := validateMessageBodyAndAttributes(input.MessageBody, input.MessageAttributes, queue.MaximumMessageSizeBytes); err != nil {
		serialize(rw, req, err)
		return
	}
	msg := queue.NewMessageStateFromSendMessageInput(input)
	queue.Push(msg)
	serialize(rw, req, msg.ForSendMessageOutput())
}

func (s Server) sendMessageBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SendMessageBatchInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	if len(input.Entries) > 10 {
		serialize(rw, req, ErrorTooManyEntriesInBatchRequest())
		return
	}
	entryIDs := apply(input.Entries, func(e types.SendMessageBatchRequestEntry) string { return safeDeref(e.Id) })
	if err := validateBatchEntryIDs(entryIDs); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	totalMessageSizeBytes := sum(apply(input.Entries, func(e types.SendMessageBatchRequestEntry) int {
		return len([]byte(safeDeref(e.MessageBody))) + messageAttributesSizeBytes(e.MessageAttributes)
	}))
	if totalMessageSizeBytes > queue.MaximumMessageSizeBytes {
		serialize(rw, req, ErrorBatchRequestTooLong().WithMessagef("Batch requests cannot be longer than %d bytes. You have sent %d bytes.", queue.MaximumMessageSizeBytes, totalMessageSizeBytes))
		return
	}

	messages := make([]*MessageState, 0, len(input.Entries))
	var failedEntries []BatchResultErrorEntry
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, req, err)
			return
		}
		if err := validateMessageBody(entry.MessageBody, queue.MaximumMessageSizeBytes); err != nil {
			failedEntries = append(failedEntries, BatchResultErrorEntry{
				Error: *err,
				ID:    safeDeref(entry.Id),
			})
			continue
		}
		// NOTE(wcharczuk): we don't need to check the body size here, as that
		// would already have been covered by the total check above!
		msg := queue.NewMessageStateFromSendMessageBatchEntry(entry)
		messages = append(messages, msg)
	}
	queue.Push(messages...)
	serialize(rw, req, &SendMessageBatchOutput{
		Successful: apply(messages, func(ms *MessageState) types.SendMessageBatchResultEntry {
			return ms.ForSendMessageBatchResultEntry()
		}),
		Failed:         failedEntries,
		ResultMetadata: middleware.Metadata{},
	})
}

func (s Server) deleteMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteMessageInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	ok = queue.Delete(*input.ReceiptHandle)
	if !ok {
		serialize(rw, req, ErrorReceiptHandleIsInvalid())
		return
	}
	serialize(rw, req, &sqs.DeleteMessageOutput{})
}

func (s Server) deleteMessageBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteMessageBatchInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, req, err)
			return
		}
		if err := requireReceiptHandle(entry.ReceiptHandle); err != nil {
			serialize(rw, req, err)
			return
		}
	}
	successful, failed := queue.DeleteBatch(input.Entries)
	serialize(rw, req, &sqs.DeleteMessageBatchOutput{
		Successful: successful,
		Failed:     failed,
	})
}

func (s Server) changeMessageVisibility(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ChangeMessageVisibilityInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	visibilityTimeout := time.Duration(input.VisibilityTimeout) * time.Second
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	ok = queue.ChangeMessageVisibility(
		safeDeref(input.ReceiptHandle),
		visibilityTimeout,
	)
	if !ok {
		serialize(rw, req, ErrorReceiptHandleIsInvalid())
		return
	}
	serialize(rw, req, &sqs.ChangeMessageVisibilityOutput{})
}

func (s Server) changeMessageVisibilityBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ChangeMessageVisibilityBatchInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, req, err)
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	if !ok {
		serialize(rw, req, ErrorQueueDoesNotExist())
		return
	}
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, req, err)
			return
		}
		if err := requireReceiptHandle(entry.ReceiptHandle); err != nil {
			serialize(rw, req, err)
			return
		}
		visibilityTimeout := time.Duration(entry.VisibilityTimeout) * time.Second
		if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
			serialize(rw, req, err)
			return
		}
	}
	successful, failed := queue.ChangeMessageVisibilityBatch(input.Entries)
	serialize(rw, req, &sqs.ChangeMessageVisibilityBatchOutput{
		Successful: successful,
		Failed:     failed,
	})
}

func (s Server) startMessageMoveTask(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.StartMessageMoveTaskInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if input.SourceArn == nil || *input.SourceArn == "" {
		serialize(rw, req, ErrorInvalidAddress().WithMessagef("SourceArn is required"))
		return
	}
	if input.MaxNumberOfMessagesPerSecond != nil && (*input.MaxNumberOfMessagesPerSecond < 0 || *input.MaxNumberOfMessagesPerSecond > 500) {
		serialize(rw, req, ErrorInvalidParameterValueException().WithMessagef("MaxNumberOfMessagesPerSecond must be less than 500, you put %d", *input.MaxNumberOfMessagesPerSecond))
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queues := s.accounts.EnsureQueues(authz.AccountID)
	mmt, err := queues.StartMoveMessageTask(s.clock, *input.SourceArn, safeDeref(input.DestinationArn), safeDeref(input.MaxNumberOfMessagesPerSecond))
	if err != nil {
		serialize(rw, req, err)
		return
	}
	serialize(rw, req, &sqs.StartMessageMoveTaskOutput{
		TaskHandle: aws.String(mmt.TaskHandle),
	})
}

func (s Server) cancelMoveMessageTask(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.CancelMessageMoveTaskInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if input.TaskHandle == nil || *input.TaskHandle == "" {
		serialize(rw, req, ErrorInvalidAddress().WithMessagef("TaskHandle is required"))
		return
	}
	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queues := s.accounts.EnsureQueues(authz.AccountID)
	task, err := queues.CancelMoveMessageTask(*input.TaskHandle)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	serialize(rw, req, &sqs.CancelMessageMoveTaskOutput{
		ApproximateNumberOfMessagesMoved: int64(task.Stats().ApproximateNumberOfMessagesMoved),
	})
}

func (s Server) listMoveMessageTasks(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ListMessageMoveTasksInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	if input.SourceArn == nil || *input.SourceArn == "" {
		serialize(rw, req, ErrorInvalidAddress().WithMessagef("SourceArn is required"))
		return
	}
	if input.MaxResults != nil && (*input.MaxResults < 0 || *input.MaxResults > 10) {
		serialize(rw, req, ErrorInvalidAttributeValue().WithMessagef("MaxResults must be greater than 0 and less than 10, you put %d", *input.MaxResults))
		return
	}

	// if unset, max results defaults to 1
	// otherwise the user can provide a max results [1,10]
	var maxResults = 1
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = int(*input.MaxResults)
	}

	authz, ok := GetContextAuthorization(req.Context())
	if !ok {
		serialize(rw, req, ErrorResponseInvalidSecurity())
		return
	}
	queues := s.accounts.EnsureQueues(authz.AccountID)
	var results []types.ListMessageMoveTasksResultEntry
	for mmt := range queues.EachMoveMessageTasks(*input.SourceArn) {
		mmtStats := mmt.Stats()

		var taskHandle string
		if mmt.Status() == MessageMoveStatusRunning {
			taskHandle = mmt.TaskHandle
		}
		results = append(results, types.ListMessageMoveTasksResultEntry{
			TaskHandle:                        aws.String(taskHandle),
			StartedTimestamp:                  mmt.Started().Unix(),
			SourceArn:                         aws.String(mmt.SourceQueue.ARN),
			DestinationArn:                    aws.String(mmt.DestinationQueue.ARN),
			MaxNumberOfMessagesPerSecond:      aws.Int32(int32(mmt.MaxNumberOfMessagesPerSecond)),
			Status:                            aws.String(mmt.Status().String()),
			ApproximateNumberOfMessagesMoved:  int64(mmtStats.ApproximateNumberOfMessagesMoved),
			ApproximateNumberOfMessagesToMove: aws.Int64(int64(mmtStats.ApproximateNumberOfMessagesToMove)),
		})
		if len(results) == maxResults {
			break
		}
	}
	serialize(rw, req, &sqs.ListMessageMoveTasksOutput{
		Results: results,
	})
}

func (s Server) addPermission(rw http.ResponseWriter, req *http.Request) {
	_, err := deserialize[sqs.AddPermissionInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	// if err := requireQueueURL(input.QueueUrl); err != nil {
	// 	serialize(rw, req, err)
	// 	return
	// }
	// authz, ok := GetContextAuthorization(req.Context())
	// if !ok {
	// 	serialize(rw, req, ErrorResponseInvalidSecurity())
	// 	return
	// }
	// queue, ok := s.accounts.EnsureQueues(authz.AccountID).GetQueue(*input.QueueUrl)
	// if !ok {
	// 	serialize(rw, req, ErrorQueueDoesNotExist())
	// 	return
	// }
	serialize(rw, req, &sqs.AddPermissionOutput{})
}

func (s Server) removePermission(rw http.ResponseWriter, req *http.Request) {
	_, err := deserialize[sqs.RemovePermissionInput](req)
	if err != nil {
		serialize(rw, req, err)
		return
	}
	serialize(rw, req, &sqs.RemovePermissionOutput{})
}

func (s Server) unknownMethod(rw http.ResponseWriter, req *http.Request) {
	serialize(rw, req, ErrorUnsupportedOperation().WithMessagef("Invalid method %s", req.Method))
}

func (s Server) unknownPath(rw http.ResponseWriter, req *http.Request) {
	serialize(rw, req, ErrorUnsupportedOperation().WithMessagef("Invalid resource %s", req.URL.Path))
}

func (s Server) invalidMethod(rw http.ResponseWriter, req *http.Request, action string) {
	serialize(rw, req, ErrorUnsupportedOperation().WithMessagef("Invalid action %s", action))
}

func requireEntryID(id *string) *Error {
	if id == nil || *id == "" {
		return ErrorInvalidAttributeValue().WithMessagef("Id")
	}
	return nil
}

func requireReceiptHandle(receiptHandle *string) *Error {
	if receiptHandle == nil || *receiptHandle == "" {
		return ErrorReceiptHandleIsInvalid()
	}
	return nil
}

func requireQueueURL(queueURL *string) *Error {
	if queueURL == nil || *queueURL == "" {
		return ErrorInvalidAddress().WithMessagef("QueueUrl")
	}
	return nil
}

func deserialize[V any](req *http.Request) (*V, *Error) {
	if req.Body == nil {
		return nil, nil
	}
	defer req.Body.Close()
	var value V
	if err := json.NewDecoder(req.Body).Decode(&value); err != nil {
		return nil, &Error{
			Type:       "com.amazonaws.sqs#InvalidInput",
			StatusCode: http.StatusBadRequest,
			Message:    fmt.Sprintf("Deserializing input failed: %v", err),
		}
	}
	return &value, nil
}

func serialize(rw http.ResponseWriter, _ *http.Request, res any) {
	rw.Header().Set("Content-Type", ContentTypeAmzJSON)
	rw.Header().Set(HeaderAmznRequestID, uuid.V4().String())
	if commonError, ok := res.(*Error); ok {
		rw.WriteHeader(commonError.StatusCode)
	} else {
		rw.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(rw).Encode(res)
}

func marshalJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
