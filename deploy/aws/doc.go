// Package deployaws holds the AWS SAM deployment of dynamodb-eventstream
// (template.yaml, samconfig.toml, local-env.json, README.md): the
// ClickHouse-compatible HTTP API on Lambda behind a private REST API
// Gateway, with the stream worker, the replicator and the reconciler as
// Lambdas on DynamoDB streams, one stack per region.
//
// The package has no Go code besides its tests, which check the template
// against the contract the binaries implement (function code directories,
// environment variable names the code reads, IAM scoped to TABLE_PREFIX,
// private endpoint configuration) and lint it with the SAM CLI when
// installed.
package deployaws
