package streams

import (
	"errors"

	"github.com/aws/smithy-go"
)

// Error codes are matched by name: DynamoDB, DynamoDB Streams and DynamoDB
// Local model the same conditions as distinct Go types.
func hasCode(err error, codes ...string) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	for _, c := range codes {
		if ae.ErrorCode() == c {
			return true
		}
	}
	return false
}

func isTableNotFound(err error) bool { return hasCode(err, "ResourceNotFoundException") }

func isStreamNotFound(err error) bool { return hasCode(err, "ResourceNotFoundException") }

// IsConditionFailed reports whether err is a DynamoDB conditional-check
// failure (ConditionalCheckFailedException).
func IsConditionFailed(err error) bool { return hasCode(err, "ConditionalCheckFailedException") }
