# variables.tf declares the configurable inputs for the loafer-awsx example
# infrastructure. Every variable has a LocalStack-friendly default so the module
# can be applied with no overrides during local development. Override them (via
# -var, a *.tfvars file, or TF_VAR_ environment variables) to target a real AWS
# account or a different region.

variable "region" {
  description = "AWS region used by the provider and when building ARNs."
  type        = string
  default     = "us-east-1"
}

variable "account_id" {
  description = "AWS account ID used when building ARNs. LocalStack uses 000000000000."
  type        = string
  default     = "000000000000"
}

variable "localstack_endpoint" {
  description = "LocalStack gateway endpoint that the SQS and SNS services are reached through."
  type        = string
  default     = "http://localhost:4566"
}

variable "access_key" {
  description = "Static access key. Any non-empty value works with LocalStack."
  type        = string
  default     = "test"
}

variable "secret_key" {
  description = "Static secret key. Any non-empty value works with LocalStack."
  type        = string
  default     = "test"
}

variable "standard_queue_name" {
  description = "Name of the standard SQS queue subscribed to the standard topic."
  type        = string
  default     = "example-basic-queue"
}

variable "fifo_queue_name" {
  description = "Name of the FIFO SQS queue subscribed to the FIFO topic. Must end with .fifo."
  type        = string
  default     = "example-fifo-queue.fifo"
}

variable "typed_queue_name" {
  description = "Name of the standard SQS queue consumed by the typed example, subscribed to the standard topic."
  type        = string
  default     = "example-typed-queue"
}

variable "middleware_queue_name" {
  description = "Name of the standard SQS queue consumed by the middleware example, subscribed to the standard topic."
  type        = string
  default     = "example-middleware-queue"
}

variable "standard_topic_name" {
  description = "Name of the standard SNS topic."
  type        = string
  default     = "example-standard-topic"
}

variable "orders_topic_name" {
  description = "Name of the standard SNS topic carrying OrderPlaced messages consumed by the typed example."
  type        = string
  default     = "example-orders-topic"
}

variable "fifo_topic_name" {
  description = "Name of the FIFO SNS topic. Must end with .fifo."
  type        = string
  default     = "example-fifo-topic.fifo"
}
