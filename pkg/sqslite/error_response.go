package sqslite

import (
	"fmt"
	"net/http"
)

func ErrorResponseInvalidAddress() *Error {
	return &Error{
		Code:        "InvalidAddress",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     "The address https://queue.amazonaws.com/ is not valid for this endpoint.",
	}
}

func ErrorResponseInvalidMethod(method string) *Error {
	return &Error{
		Code:        "InvalidMethod",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     fmt.Sprintf("The http method %s is not valid for this endpoint.", method),
	}
}

func ErrorResponseInvalidAction(action string) *Error {
	return &Error{
		Code:        "InvalidAction",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     fmt.Sprintf("The action %s is not valid for this endpoint.", action),
	}
}

func ErrorUnknownOperation(message string) *Error {
	return &Error{
		Code:        "UnknownOperation",
		StatusCode:  http.StatusNotFound,
		SenderFault: true,
		Message:     message,
	}
}

func ErrorUnauthorized() *Error {
	return &Error{
		Code:        "Unauthorized",
		StatusCode:  http.StatusUnauthorized,
		SenderFault: true,
		Message:     "Invalid Authorization",
	}
}

func ErrorInvalidAttributeValue(message string) *Error {
	return &Error{
		Code:        "InvalidAttributeValue",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     message,
	}
}

func ErrorMissingRequiredParameter(message string) *Error {
	return &Error{
		Code:        "MissingRequiredParameter",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     message,
	}
}

func ErrorInvalidParameterValue(message string) *Error {
	return &Error{
		Code:        "InvalidParameterValue",
		StatusCode:  http.StatusBadRequest,
		SenderFault: true,
		Message:     message,
	}
}

func ErrorInternalServer(message string) *Error {
	return &Error{
		Code:        "InternalServerError",
		StatusCode:  http.StatusInternalServerError,
		SenderFault: true,
		Message:     message,
	}
}

type Error struct {
	Code        string
	StatusCode  int
	Message     string
	SenderFault bool
}

func (e Error) Status() int {
	return e.StatusCode
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
