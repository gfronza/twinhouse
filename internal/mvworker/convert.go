package mvworker

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gfronza/twinhouse/internal/streams"
)

// changesFromStream converts DynamoDB Streams records.
func changesFromStream(recs []sttypes.Record) ([]change, error) {
	out := make([]change, 0, len(recs))
	for _, r := range recs {
		c := change{EventName: string(r.EventName)}
		if sr := r.Dynamodb; sr != nil {
			if sr.SequenceNumber != nil {
				c.Seq = *sr.SequenceNumber
			}
			if sr.ApproximateCreationDateTime != nil {
				c.Created = *sr.ApproximateCreationDateTime
			}
			if c.EventName != eventRemove {
				img, err := streams.ToDynamoDBItem(sr.NewImage)
				if err != nil {
					return nil, fmt.Errorf("record %s: %w", c.Seq, err)
				}
				c.NewImage = img
				c.WriteTime = writeTime(img, time.Time{})
				c.Created = writeTime(img, c.Created)
			}
		}
		out = append(out, c)
	}
	return out, nil
}

// changeFromLambda converts one Lambda DynamoDB event record.
func changeFromLambda(r events.DynamoDBEventRecord) (change, error) {
	c := change{
		EventName: r.EventName,
		Seq:       r.Change.SequenceNumber,
		Created:   r.Change.ApproximateCreationDateTime.Time,
	}
	if c.EventName == eventRemove {
		return c, nil
	}
	img, err := FromLambdaItem(r.Change.NewImage)
	if err != nil {
		return c, fmt.Errorf("record %s: %w", c.Seq, err)
	}
	c.NewImage = img
	c.WriteTime = writeTime(img, time.Time{})
	c.Created = writeTime(img, c.Created)
	return c, nil
}

// writeTime returns the row's write time (_ts, unix nanos), which is more
// precise than a stream record's ApproximateCreationDateTime (DynamoDB Local
// truncates it to the minute), falling back to def.
func writeTime(img map[string]ddbtypes.AttributeValue, def time.Time) time.Time {
	if v, ok := img["_ts"].(*ddbtypes.AttributeValueMemberN); ok {
		if n, err := strconv.ParseInt(v.Value, 10, 64); err == nil && n > 0 {
			return time.Unix(0, n)
		}
	}
	return def
}

// FromLambdaItem converts a Lambda event image to DynamoDB attribute values.
func FromLambdaItem(img map[string]events.DynamoDBAttributeValue) (map[string]ddbtypes.AttributeValue, error) {
	if img == nil {
		return nil, nil
	}
	out := make(map[string]ddbtypes.AttributeValue, len(img))
	for k, v := range img {
		c, err := FromLambda(v)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = c
	}
	return out, nil
}

// FromLambda converts a Lambda event attribute value.
func FromLambda(av events.DynamoDBAttributeValue) (ddbtypes.AttributeValue, error) {
	switch av.DataType() {
	case events.DataTypeString:
		return &ddbtypes.AttributeValueMemberS{Value: av.String()}, nil
	case events.DataTypeNumber:
		return &ddbtypes.AttributeValueMemberN{Value: av.Number()}, nil
	case events.DataTypeBinary:
		return &ddbtypes.AttributeValueMemberB{Value: av.Binary()}, nil
	case events.DataTypeBoolean:
		return &ddbtypes.AttributeValueMemberBOOL{Value: av.Boolean()}, nil
	case events.DataTypeNull:
		return &ddbtypes.AttributeValueMemberNULL{Value: true}, nil
	case events.DataTypeStringSet:
		return &ddbtypes.AttributeValueMemberSS{Value: av.StringSet()}, nil
	case events.DataTypeNumberSet:
		return &ddbtypes.AttributeValueMemberNS{Value: av.NumberSet()}, nil
	case events.DataTypeBinarySet:
		return &ddbtypes.AttributeValueMemberBS{Value: av.BinarySet()}, nil
	case events.DataTypeList:
		l := av.List()
		out := make([]ddbtypes.AttributeValue, len(l))
		for i, e := range l {
			c, err := FromLambda(e)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return &ddbtypes.AttributeValueMemberL{Value: out}, nil
	case events.DataTypeMap:
		m, err := FromLambdaItem(av.Map())
		if err != nil {
			return nil, err
		}
		if m == nil {
			m = map[string]ddbtypes.AttributeValue{}
		}
		return &ddbtypes.AttributeValueMemberM{Value: m}, nil
	default:
		return nil, fmt.Errorf("unsupported Lambda attribute data type %d", av.DataType())
	}
}

// TableFromStreamARN extracts the table name from a stream (or event source)
// ARN: arn:aws:dynamodb:<region>:<account>:table/<name>/stream/<label>.
func TableFromStreamARN(arn string) (string, bool) {
	_, rest, ok := strings.Cut(arn, ":table/")
	if !ok {
		return "", false
	}
	name, _, ok := strings.Cut(rest, "/stream/")
	if !ok || name == "" {
		return "", false
	}
	return name, true
}
