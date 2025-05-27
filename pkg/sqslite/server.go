package sqslite

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/wcharczuk/sqslite/pkg/uuid"
)

// NewServer returns a new server.
func NewServer(options ...ServerOption) *Server {
	s := Server{
		baseQueueURL: "http://sqslite.us-west-2.localhost",
		queues:       NewQueues(),
	}
	for _, opt := range options {
		opt(&s)
	}
	return &s
}

// ServerOption is a function that mutates servers.
type ServerOption func(*Server)

// OptBaseQueueURL sets the base QueueURL for newly created queues.
func OptBaseQueueURL(baseQueueURL string) ServerOption {
	return func(s *Server) {
		s.baseQueueURL = baseQueueURL
	}
}

var _ http.Handler = (*Server)(nil)

// Server implements the http routing layer for sqslite.
type Server struct {
	baseQueueURL string
	queues       *Queues
}

// BaseQueueURL returns the base queue url.
func (s *Server) BaseQueueURL() string {
	return s.baseQueueURL
}

// Queues returns the underlying queues storage.
func (s *Server) Queues() *Queues {
	return s.queues
}

// EachQueue returns an iterator for the queues in the server.
func (s *Server) EachQueue() iter.Seq[*Queue] {
	return func(yield func(*Queue) bool) {
		s.queues.queuesMu.Lock()
		defer s.queues.queuesMu.Unlock()
		for _, q := range s.queues.queues {
			if !yield(q) {
				return
			}
		}
	}
}

// Close shuts down the server.
func (s *Server) Close() {
	s.queues.Close()
}

const (
	methodCreateQueue                  = "AmazonSQS.CreateQueue"
	methodListQueues                   = "AmazonSQS.ListQueues"
	methodSetQueueAttributes           = "AmazonSQS.SetQueueAttributes"
	methodTagQueue                     = "AmazonSQS.TagQueue"
	methodUntagQueue                   = "AmazonSQS.UntagQueue"
	methodPurgeQueue                   = "AmazonSQS.PurgeQueue"
	methodDeleteQueue                  = "AmazonSQS.DeleteQueue"
	methodReceiveMessage               = "AmazonSQS.ReceiveMessage"
	methodSendMessage                  = "AmazonSQS.SendMessage"
	methodDeleteMessage                = "AmazonSQS.DeleteMessage"
	methodChangeMessageVisibility      = "AmazonSQS.ChangeMessageVisibility"
	methodSendMessageBatch             = "AmazonSQS.SendMessageBatch"
	methodDeleteMessageBatch           = "AmazonSQS.DeleteMessageBatch"
	methodChangeMessageVisibilityBatch = "AmazonSQS.ChangeMessageVisibilityBatch"
)

// ServeHTTP implements [http.Handler].
func (s Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		s.unknownVerb(rw, req)
		return
	}
	if req.URL.Path != "/" {
		s.unknownPath(rw, req)
		return
	}
	action := req.Header.Get("X-Amz-Target")
	switch action {
	case methodCreateQueue:
		s.createQueue(rw, req)
	case methodSetQueueAttributes:
		s.setQueueAttributes(rw, req)
	case methodTagQueue:
		s.tagQueue(rw, req)
	case methodUntagQueue:
		s.untagQueue(rw, req)
	case methodPurgeQueue:
		s.purgeQueue(rw, req)
	case methodDeleteQueue:
		s.deleteQueue(rw, req)
	case methodReceiveMessage:
		s.receiveMessage(rw, req)
	case methodSendMessage:
		s.sendMessage(rw, req)
	case methodSendMessageBatch:
		s.sendMessageBatch(rw, req)
	case methodDeleteMessage:
		s.deleteMessage(rw, req)
	case methodDeleteMessageBatch:
		s.deleteMessageBatch(rw, req)
	case methodChangeMessageVisibility:
		s.changeMessageVisibility(rw, req)
	case methodChangeMessageVisibilityBatch:
		s.changeMessageVisibilityBatch(rw, req)
	default:
		s.invalidMethod(rw, action)
	}
}

func (s Server) createQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.CreateQueueInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	queue, err := NewQueueFromCreateQueueInput(s.baseQueueURL, input)
	if err != nil {
		serialize(rw, err)
		return
	}
	err = s.queues.AddQueue(req.Context(), queue)
	if err != nil {
		serialize(rw, err)
		return
	}
	queue.Start()
	serialize(rw, &sqs.CreateQueueOutput{
		QueueUrl: &queue.URL,
	})
}

func (s Server) setQueueAttributes(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SetQueueAttributesInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	if err = queue.SetQueueAttributes(input.Attributes); err != nil {
		serialize(rw, err)
		return
	}
	serialize(rw, &sqs.SetQueueAttributesOutput{})
}

func (s Server) tagQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.TagQueueInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	queue.Tag(input.Tags)
	serialize(rw, &sqs.TagQueueOutput{})
}

func (s Server) untagQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.UntagQueueInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	queue.Untag(input.TagKeys)
	serialize(rw, &sqs.UntagQueueOutput{})
}

func (s Server) purgeQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.PurgeQueueInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	ok := s.queues.PurgeQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	serialize(rw, &sqs.PurgeQueueOutput{})
}

func (s Server) deleteQueue(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteQueueInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	ok := s.queues.DeleteQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	serialize(rw, &sqs.DeleteQueueOutput{})
}

func (s Server) receiveMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ReceiveMessageInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	visibilityTimeout := time.Duration(input.VisibilityTimeout) * time.Second
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		serialize(rw, err)
		return
	}
	waitTimeout := time.Duration(input.WaitTimeSeconds) * time.Second
	if err := validateWaitTimeSeconds(waitTimeout); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	allMessages := queue.Receive(int(input.MaxNumberOfMessages), visibilityTimeout)
	if len(allMessages) < int(input.MaxNumberOfMessages) {
		var waitTime time.Duration
		if input.WaitTimeSeconds > 0 {
			waitTime = waitTimeout
		} else {
			waitTime = queue.ReceiveMessageWaitTime
		}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		waitDeadline := time.NewTimer(waitTime)
		defer waitDeadline.Stop()

	done:
		for {
			select {
			case <-req.Context().Done():
				return
			case <-waitDeadline.C:
				break done
			case <-ticker.C:
				messages := queue.Receive(int(input.MaxNumberOfMessages)-len(allMessages), visibilityTimeout)
				allMessages = append(allMessages, messages...)
				if len(allMessages) == int(input.MaxNumberOfMessages) {
					break done
				}
			}
		}
	}
	serialize(rw, &sqs.ReceiveMessageOutput{
		Messages: apply(allMessages, asTypesMessage),
	})
}

func (s Server) sendMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SendMessageInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	if err := validateMessageBodySize(input.MessageBody, queue.MaximumMessageSizeBytes); err != nil {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	msg, err := queue.NewMessageState(NewMessageFromSendMessageInput(input), time.Now().UTC(), int(input.DelaySeconds))
	if err != nil {
		serialize(rw, err)
		return
	}
	queue.Push(msg)
	serialize(rw, &sqs.SendMessageOutput{
		MessageId:              aws.String(msg.Message.MessageID.String()),
		MD5OfMessageAttributes: aws.String(msg.Message.MD5OfMessageAttributes.Value),
		MD5OfMessageBody:       aws.String(msg.Message.MD5OfBody.Value),
		SequenceNumber:         aws.String(fmt.Sprint(msg.SequenceNumber)),
	})
}

func (s Server) sendMessageBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.SendMessageBatchInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	if len(input.Entries) > 10 {
		serialize(rw, ErrorInvalidParameterValue(fmt.Sprintf("Entries must have at most 10 entries, you provided %d", len(input.Entries))))
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	messages := make([]*MessageState, 0, len(input.Entries))
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, err)
			return
		}
		if err := validateMessageBodySize(entry.MessageBody, queue.MaximumMessageSizeBytes); err != nil {
			serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
			return
		}
		msg, err := queue.NewMessageState(NewMessageFromSendMessageBatchEntry(entry), time.Now().UTC(), int(entry.DelaySeconds))
		if err != nil {
			serialize(rw, err)
			return
		}
		messages = append(messages, msg)
	}
	queue.Push(messages...)
	serialize(rw, &sqs.SendMessageBatchOutput{
		Successful: apply(messages, asTypesSendMessageBatchResultEntry),
	})
}

func (s Server) deleteMessage(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteMessageInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	ok = queue.Delete(*input.ReceiptHandle)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("ReceiptHandle"))
		return
	}
	serialize(rw, &sqs.DeleteMessageOutput{})
}

func (s Server) deleteMessageBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.DeleteMessageBatchInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, err)
			return
		}
		if err := requireReceiptHandle(entry.ReceiptHandle); err != nil {
			serialize(rw, err)
			return
		}
	}
	successful, failed := queue.DeleteBatch(input.Entries)
	serialize(rw, &sqs.DeleteMessageBatchOutput{
		Successful: successful,
		Failed:     failed,
	})
}

func (s Server) changeMessageVisibility(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ChangeMessageVisibilityInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	visibilityTimeout := time.Duration(input.VisibilityTimeout) * time.Second
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	ok = queue.ChangeMessageVisibility(
		safeDeref(input.ReceiptHandle),
		visibilityTimeout,
	)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("ReceiptHandle"))
		return
	}
	serialize(rw, &sqs.ChangeMessageVisibilityOutput{})
}

func (s Server) changeMessageVisibilityBatch(rw http.ResponseWriter, req *http.Request) {
	input, err := deserialize[sqs.ChangeMessageVisibilityBatchInput](req)
	if err != nil {
		serialize(rw, err)
		return
	}
	if err := requireQueueURL(input.QueueUrl); err != nil {
		serialize(rw, err)
		return
	}
	queue, ok := s.queues.GetQueue(req.Context(), *input.QueueUrl)
	if !ok {
		serialize(rw, ErrorInvalidParameterValue("QueueUrl"))
		return
	}
	for _, entry := range input.Entries {
		if err := requireEntryID(entry.Id); err != nil {
			serialize(rw, err)
			return
		}
		if err := requireReceiptHandle(entry.ReceiptHandle); err != nil {
			serialize(rw, err)
			return
		}
		visibilityTimeout := time.Duration(entry.VisibilityTimeout) * time.Second
		if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
			serialize(rw, err)
			return
		}
	}
	successful, failed := queue.ChangeMessageVisibilityBatch(input.Entries)
	serialize(rw, &sqs.ChangeMessageVisibilityBatchOutput{
		Successful: successful,
		Failed:     failed,
	})
}

func (s Server) unknownVerb(rw http.ResponseWriter, req *http.Request) {
	serialize(rw, ErrorUnknownOperation(fmt.Sprintf("Expected HTTP POST as verb, you used: %v", req.Method)))
}

func (s Server) unknownPath(rw http.ResponseWriter, req *http.Request) {
	serialize(rw, ErrorUnknownOperation(fmt.Sprintf("Expected '/' as the request path, you used: %v", req.URL.Path)))
}

func (s Server) invalidMethod(rw http.ResponseWriter, action string) {
	serialize(rw, ErrorUnknownOperation(action))
}

func requireEntryID(id *string) *Error {
	if id == nil || *id == "" {
		return ErrorMissingRequiredParameter("Id")
	}
	return nil
}

func requireReceiptHandle(receiptHandle *string) *Error {
	if receiptHandle == nil || *receiptHandle == "" {
		return ErrorMissingRequiredParameter("ReceiptHandle")
	}
	return nil
}

func requireQueueURL(queueURL *string) *Error {
	if queueURL == nil || *queueURL == "" {
		return ErrorMissingRequiredParameter("QueueUrl")
	}
	return nil
}

func apply[Input, Output any](values []Input, fn func(Input) Output) (output []Output) {
	output = make([]Output, len(values))
	for index, input := range values {
		output[index] = fn(input)
	}
	return
}

func asTypesMessage(m Message) types.Message {
	return types.Message{
		Attributes:             m.Attributes,
		Body:                   aws.String(m.Body.Value),
		MessageId:              aws.String(m.MessageID.String()),
		ReceiptHandle:          aws.String(m.ReceiptHandle.Value),
		MD5OfBody:              aws.String(m.MD5OfBody.Value),
		MD5OfMessageAttributes: aws.String(m.MD5OfMessageAttributes.Value),
	}
}

func asTypesSendMessageBatchResultEntry(m *MessageState) types.SendMessageBatchResultEntry {
	return types.SendMessageBatchResultEntry{
		Id:                     aws.String(m.Message.ID),
		MessageId:              aws.String(m.Message.MessageID.String()),
		MD5OfMessageBody:       aws.String(m.Message.MD5OfBody.Value),
		MD5OfMessageAttributes: aws.String(m.Message.MD5OfMessageAttributes.Value),
	}
}

func md5sum(values ...string) string {
	hf := md5.New()
	for _, v := range values {
		hf.Write([]byte(v))
	}
	return hex.EncodeToString(hf.Sum(nil))
}

func deserialize[V any](req *http.Request) (*V, *Error) {
	if req.Body == nil {
		return nil, nil
	}
	defer req.Body.Close()
	var value V
	if err := json.NewDecoder(req.Body).Decode(&value); err != nil {
		return nil, &Error{
			Code:        "InvalidInput",
			SenderFault: true,
			StatusCode:  http.StatusBadRequest,
			Message:     fmt.Sprintf("Deserializing input failed: %v", err),
		}
	}
	return &value, nil
}

func serialize(rw http.ResponseWriter, res any) {
	rw.Header().Set("X-Amzn-Requestid", uuid.V4().String())
	rw.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if commonError, ok := res.(*Error); ok {
		rw.WriteHeader(commonError.StatusCode)
	} else {
		rw.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(rw).Encode(res)
}
