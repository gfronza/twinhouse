package streams

import (
	"reflect"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func TestToDynamoDBItem(t *testing.T) {
	in := map[string]sttypes.AttributeValue{
		"s":    &sttypes.AttributeValueMemberS{Value: ""},
		"n":    &sttypes.AttributeValueMemberN{Value: "-1.5e3"},
		"b":    &sttypes.AttributeValueMemberB{Value: []byte{0, 1, 0}},
		"bool": &sttypes.AttributeValueMemberBOOL{Value: true},
		"null": &sttypes.AttributeValueMemberNULL{Value: true},
		"ss":   &sttypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"ns":   &sttypes.AttributeValueMemberNS{Value: []string{"1"}},
		"bs":   &sttypes.AttributeValueMemberBS{Value: [][]byte{{0}}},
		"l": &sttypes.AttributeValueMemberL{Value: []sttypes.AttributeValue{
			&sttypes.AttributeValueMemberN{Value: "1"},
			&sttypes.AttributeValueMemberL{Value: []sttypes.AttributeValue{}},
		}},
		"m": &sttypes.AttributeValueMemberM{Value: map[string]sttypes.AttributeValue{
			"x": &sttypes.AttributeValueMemberS{Value: "y"},
		}},
	}
	want := map[string]ddbtypes.AttributeValue{
		"s":    &ddbtypes.AttributeValueMemberS{Value: ""},
		"n":    &ddbtypes.AttributeValueMemberN{Value: "-1.5e3"},
		"b":    &ddbtypes.AttributeValueMemberB{Value: []byte{0, 1, 0}},
		"bool": &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"null": &ddbtypes.AttributeValueMemberNULL{Value: true},
		"ss":   &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"ns":   &ddbtypes.AttributeValueMemberNS{Value: []string{"1"}},
		"bs":   &ddbtypes.AttributeValueMemberBS{Value: [][]byte{{0}}},
		"l": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
			&ddbtypes.AttributeValueMemberN{Value: "1"},
			&ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{}},
		}},
		"m": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"x": &ddbtypes.AttributeValueMemberS{Value: "y"},
		}},
	}
	got, err := ToDynamoDBItem(in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestToDynamoDBItemNilAndErrors(t *testing.T) {
	if got, err := ToDynamoDBItem(nil); got != nil || err != nil {
		t.Fatalf("nil image: %v %v", got, err)
	}
	bad := map[string]sttypes.AttributeValue{"x": &sttypes.UnknownUnionMember{Tag: "Q"}}
	if _, err := ToDynamoDBItem(bad); err == nil {
		t.Fatal("unknown member: expected error")
	}
	nested := map[string]sttypes.AttributeValue{"l": &sttypes.AttributeValueMemberL{Value: []sttypes.AttributeValue{nil}}}
	if _, err := ToDynamoDBItem(nested); err == nil {
		t.Fatal("nil nested value: expected error")
	}
}
