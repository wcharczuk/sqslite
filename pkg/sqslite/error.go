package sqslite

func InvalidAddress() *CommonError {
	return &CommonError{
		Code:        "InvalidAddress",
		StatusCode:  400,
		SenderFault: true,
		Message:     "The address https://queue.amazonaws.com/ is not valid for this endpoint.",
	}
}

func InvalidParameterValueException(message string) *CommonError {
	return &CommonError{
		Code:        "InvalidParameterValueException",
		StatusCode:  400,
		SenderFault: true,
		Message:     message,
	}
}

func InvalidAttributeValue(message string) *CommonError {
	return &CommonError{
		Code:        "InvalidAttributeValue",
		StatusCode:  400,
		SenderFault: true,
		Message:     message,
	}
}

func MissingRequiredParameterException(message string) *CommonError {
	return &CommonError{
		Code:        "MissingRequiredParameterException",
		StatusCode:  400,
		SenderFault: true,
		Message:     message,
	}
}

type CommonError struct {
	Code        string
	StatusCode  int
	Message     string
	SenderFault bool
}
