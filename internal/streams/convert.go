package streams

import (
	"fmt"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// ToDynamoDB converts a DynamoDB Streams attribute value into the equivalent
// DynamoDB attribute value (the SDK models them as distinct types).
func ToDynamoDB(av sttypes.AttributeValue) (ddbtypes.AttributeValue, error) {
	switch v := av.(type) {
	case *sttypes.AttributeValueMemberS:
		return &ddbtypes.AttributeValueMemberS{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberN:
		return &ddbtypes.AttributeValueMemberN{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberB:
		return &ddbtypes.AttributeValueMemberB{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberBOOL:
		return &ddbtypes.AttributeValueMemberBOOL{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberNULL:
		return &ddbtypes.AttributeValueMemberNULL{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberSS:
		return &ddbtypes.AttributeValueMemberSS{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberNS:
		return &ddbtypes.AttributeValueMemberNS{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberBS:
		return &ddbtypes.AttributeValueMemberBS{Value: v.Value}, nil
	case *sttypes.AttributeValueMemberL:
		out := make([]ddbtypes.AttributeValue, len(v.Value))
		for i, e := range v.Value {
			c, err := ToDynamoDB(e)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return &ddbtypes.AttributeValueMemberL{Value: out}, nil
	case *sttypes.AttributeValueMemberM:
		m, err := ToDynamoDBItem(v.Value)
		if err != nil {
			return nil, err
		}
		return &ddbtypes.AttributeValueMemberM{Value: m}, nil
	case nil:
		return nil, fmt.Errorf("streams: nil attribute value")
	default:
		return nil, fmt.Errorf("streams: unsupported attribute value type %T", av)
	}
}

// ToDynamoDBItem converts a stream image (NewImage/OldImage/Keys) into a
// DynamoDB item. A nil image converts to nil.
func ToDynamoDBItem(img map[string]sttypes.AttributeValue) (map[string]ddbtypes.AttributeValue, error) {
	if img == nil {
		return nil, nil
	}
	out := make(map[string]ddbtypes.AttributeValue, len(img))
	for k, v := range img {
		c, err := ToDynamoDB(v)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = c
	}
	return out, nil
}
