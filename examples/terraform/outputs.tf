# outputs.tf exposes the identifiers examples and integration tests need to
# reach the provisioned resources: queue URLs and ARNs, and topic ARNs.

output "standard_queue_url" {
  description = "URL of the standard SQS queue."
  value       = aws_sqs_queue.standard.id
}

output "standard_queue_arn" {
  description = "ARN of the standard SQS queue."
  value       = aws_sqs_queue.standard.arn
}

output "fifo_queue_url" {
  description = "URL of the FIFO SQS queue."
  value       = aws_sqs_queue.fifo.id
}

output "fifo_queue_arn" {
  description = "ARN of the FIFO SQS queue."
  value       = aws_sqs_queue.fifo.arn
}

output "typed_queue_url" {
  description = "URL of the typed example SQS queue."
  value       = aws_sqs_queue.typed.id
}

output "typed_queue_arn" {
  description = "ARN of the typed example SQS queue."
  value       = aws_sqs_queue.typed.arn
}

output "middleware_queue_url" {
  description = "URL of the middleware example SQS queue."
  value       = aws_sqs_queue.middleware.id
}

output "middleware_queue_arn" {
  description = "ARN of the middleware example SQS queue."
  value       = aws_sqs_queue.middleware.arn
}

output "standard_topic_arn" {
  description = "ARN of the standard SNS topic."
  value       = aws_sns_topic.standard.arn
}

output "orders_topic_arn" {
  description = "ARN of the orders SNS topic consumed by the typed example."
  value       = aws_sns_topic.orders.arn
}

output "fifo_topic_arn" {
  description = "ARN of the FIFO SNS topic."
  value       = aws_sns_topic.fifo.arn
}
