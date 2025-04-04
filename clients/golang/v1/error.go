package v1

import (
	"fmt"
)

type AmqError struct {
	Code    int
	Message string
}

func (e AmqError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

var (
	ErrInvalidAccessKeyOrSecret = AmqError{100, "Invalid access key or secret"}

	ConsumeAck     = AmqError{200, "Consume ack"}
	ReconsumeLater = AmqError{201, "Reconsume later"}
)
