package sqslite

import (
	"fmt"
	"net/http"
)

func ErrorInvalidAddress() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidAddress",
	}
}

func ErrorInvalidParameterValueException() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidParameterValueException",
	}
}

func ErrorQueueNameAlreadyExists() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#QueueNameExists",
	}
}

func ErrorQueueDoesNotExist() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#QueueDoesNotExist",
	}
}

func ErrorQueueDeletedRecently() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#QueueDeletedRecently",
	}
}

func ErrorResponseInvalidSecurity() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidSecurity",
	}
}

func ErrorInvalidAttributeName() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidAttributeName ",
	}
}

func ErrorInvalidAttributeValue() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidAttributeValue",
	}
}

func ErrorInvalidMessageContents() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#InvalidMessageContents ",
	}
}

func ErrorUnsupportedOperation() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#UnsupportedOperation",
	}
}

func ErrorResourceNotFoundException() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#ResourceNotFoundException",
	}
}

func ErrorReceiptHandleIsInvalid() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#ReceiptHandleIsInvalid ",
	}
}

func ErrorInternalServer() *Error {
	return &Error{
		StatusCode: http.StatusInternalServerError,
		Type:       "com.amazonaws.sqs#InternalServerError",
	}
}

func ErrorTooManyEntriesInBatchRequest() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#TooManyEntriesInBatchRequest ",
	}
}

func ErrorBatchEntryIdsNotDistinct() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#BatchEntryIdsNotDistinct",
	}
}

func ErrorInvalidBatchEntryID() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs# InvalidBatchEntryId ",
	}
}

func ErrorBatchRequestTooLong() *Error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Type:       "com.amazonaws.sqs#BatchRequestTooLong",
	}
}

type Error struct {
	StatusCode int    `json:"-"`
	Type       string `json:"__type"`
	Message    string `json:"message"`
}

func (e Error) WithMessage(message string) *Error {
	e.Message = message
	return &e
}

func (e Error) WithMessagef(format string, args ...any) *Error {
	e.Message = fmt.Sprintf(format, args...)
	return &e
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}
