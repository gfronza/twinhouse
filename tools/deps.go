//go:build tools

// Package tools pins module dependencies shared by packages that are built in
// parallel, so go.mod does not need concurrent edits.
package tools

import (
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/andybalholm/brotli"
	_ "github.com/aws/aws-lambda-go/lambda"
	_ "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	_ "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	_ "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	_ "github.com/aws/aws-sdk-go-v2/service/lambda"
	_ "github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	_ "github.com/google/uuid"
	_ "github.com/klauspost/compress/zstd"
	_ "github.com/pierrec/lz4/v4"
)
