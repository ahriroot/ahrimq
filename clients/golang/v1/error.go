// Package v1 provides error definitions for the Ahrimq Go client.
// These errors are used for client-server communication and callback handling.
package v1

import (
	"fmt"
)

// AmqError represents a custom error type for Ahrimq client operations.
// It contains an error code, human-readable message, and optional delay parameter.
type AmqError struct {
	Code    int    // Error code for machine-readable error handling
	Message string // Human-readable error message
	Delay   uint64 // Optional delay in seconds for reconsume operations
}

// Error implements the error interface for AmqError.
// Returns a formatted string with error code and message.
func (e AmqError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

// Error code constants for common Ahrimq client operations.
// These codes are used to identify different error types.
const (
	// ConsumeAckCode is the error code for successful message consumption.
	ConsumeAckCode = 200

	// ReconsumeLaterCode is the error code for requesting message reconsumption later.
	ReconsumeLaterCode = 201

	// ReconsumeDelayCode is the error code for requesting message reconsumption with a delay.
	ReconsumeDelayCode = 202
)

// Predefined error variables and functions for common Ahrimq client operations.
var (
	// ErrInvalidAccessKeyOrSecret is returned when the provided access key or secret is invalid.
	// Code: 100
	// Delay: 0 (not applicable for this error)
	ErrInvalidAccessKeyOrSecret = AmqError{100, "Invalid access key or secret", 0}

	// ConsumeAck is used in callbacks to indicate that a message has been successfully consumed.
	// When returned from a message callback, it triggers an acknowledgment to the server.
	// Code: 200
	// Delay: 0 (not applicable for acknowledgments)
	ConsumeAck = AmqError{ConsumeAckCode, "Consume ack", 0}

	// ReconsumeLater is used in callbacks to indicate that a message should be reconsumed later.
	// When returned from a message callback, it triggers a reconsumption request to the server.
	// Code: 201
	// Delay: 0 (server will use default delay)
	ReconsumeLater = AmqError{ReconsumeLaterCode, "Reconsume later", 0}

	// ReconsumeDelay is a function that creates an AmqError for requesting delayed reconsumption.
	// When returned from a message callback, it triggers a delayed reconsumption request to the server.
	// Code: 202
	// Delay: User-specified delay in seconds
	// Example: ReconsumeDelay(60) for 60 seconds delay
	ReconsumeDelay = func(delay uint64) AmqError {
		return AmqError{ReconsumeDelayCode, "Reconsume delay", delay}
	}
)
