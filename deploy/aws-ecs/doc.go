// Package deployawsecs holds the container deployment of
// dynamodb-eventstream on Amazon ECS Fargate (template.yaml,
// samconfig.toml, README.md): chserver behind an internal Application Load
// Balancer on port 8123, the mvworker stream worker (or chserver's
// in-process worker) and, once per deployment, the replicator, one plain
// CloudFormation stack per region in an existing VPC.
//
// The package has no Go code besides its tests. They parse the template
// with the standard library (cfn_test.go) and check it against the
// contract the binaries implement: every environment variable a container
// sets is read by the code, chserver listens on the port the target group
// and listener use, the health check is /ping, CH_USERS comes from Secrets
// Manager, the task roles are scoped to TABLE_PREFIX, and the optional
// services follow their conditions. They lint the template with the SAM
// CLI when installed. The ecslocal build tag adds a test that runs the
// rendered task definitions as local containers against DynamoDB Local
// (see README.md, "Local check without AWS").
package deployawsecs
