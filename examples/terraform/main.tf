# main.tf provisions all AWS resources the loafer-awsx examples need, targeting
# a local LocalStack instance. It creates a standard and a FIFO SQS queue, a
# standard and a FIFO SNS topic, wires each topic to its matching queue with an
# SNS-to-SQS subscription, and attaches the SQS queue policies that allow SNS to
# deliver messages.
#
# The AWS provider is pointed at LocalStack: credential, metadata, and
# account-ID lookups are skipped, static test credentials are used, and the SQS
# and SNS service endpoints are overridden to the LocalStack gateway.

terraform {
  required_version = ">= 1.3"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region     = var.region
  access_key = var.access_key
  secret_key = var.secret_key

  # LocalStack does not implement the credential/metadata/account validation
  # calls the real AWS provider makes on start-up, so they are skipped here.
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # Route the only two services this module uses to the LocalStack gateway.
  endpoints {
    sqs = var.localstack_endpoint
    sns = var.localstack_endpoint
  }
}

# --- SQS queues -------------------------------------------------------------

# Standard queue: the destination for messages fanned out from the standard
# topic.
resource "aws_sqs_queue" "standard" {
  name = var.standard_queue_name
}

# FIFO queue: the destination for messages fanned out from the FIFO topic. FIFO
# queues require the .fifo suffix and fifo_queue = true. Content-based
# deduplication lets producers omit an explicit deduplication ID.
resource "aws_sqs_queue" "fifo" {
  name                        = var.fifo_queue_name
  fifo_queue                  = true
  content_based_deduplication = true
}

# Typed queue: the destination consumed by the typed example. It is subscribed
# to the standard topic so the producer's standard publishes fan out to it.
resource "aws_sqs_queue" "typed" {
  name = var.typed_queue_name
}

# Middleware queue: the destination consumed by the middleware example. It is
# subscribed to the standard topic so the producer's standard publishes fan out
# to it.
resource "aws_sqs_queue" "middleware" {
  name = var.middleware_queue_name
}

# --- SNS topics -------------------------------------------------------------

# Standard topic.
resource "aws_sns_topic" "standard" {
  name = var.standard_topic_name
}

# Orders topic: carries OrderPlaced-shaped messages consumed by the typed
# example. A dedicated topic keeps the typed queue free of the unrelated user
# events published to the standard topic.
resource "aws_sns_topic" "orders" {
  name = var.orders_topic_name
}

# FIFO topic: requires the .fifo suffix and fifo_topic = true. Content-based
# deduplication mirrors the FIFO queue setting.
resource "aws_sns_topic" "fifo" {
  name                        = var.fifo_topic_name
  fifo_topic                  = true
  content_based_deduplication = true
}

# --- SQS queue policies (allow SNS to deliver) ------------------------------

# Allow the standard topic to send messages to the standard queue.
resource "aws_sqs_queue_policy" "standard" {
  queue_url = aws_sqs_queue.standard.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSDelivery"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.standard.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.standard.arn
          }
        }
      }
    ]
  })
}

# Allow the FIFO topic to send messages to the FIFO queue.
resource "aws_sqs_queue_policy" "fifo" {
  queue_url = aws_sqs_queue.fifo.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSDelivery"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.fifo.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.fifo.arn
          }
        }
      }
    ]
  })
}

# Allow the orders topic to send messages to the typed queue.
resource "aws_sqs_queue_policy" "typed" {
  queue_url = aws_sqs_queue.typed.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSDelivery"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.typed.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.orders.arn
          }
        }
      }
    ]
  })
}

# Allow the standard topic to send messages to the middleware queue.
resource "aws_sqs_queue_policy" "middleware" {
  queue_url = aws_sqs_queue.middleware.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSDelivery"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.middleware.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.standard.arn
          }
        }
      }
    ]
  })
}

# --- SNS-to-SQS subscriptions ----------------------------------------------

# Standard topic -> standard queue. raw_message_delivery = true forwards the
# published message verbatim instead of wrapping it in the SNS envelope.
resource "aws_sns_topic_subscription" "standard" {
  topic_arn            = aws_sns_topic.standard.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.standard.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.standard]
}

# Orders topic -> typed queue. Delivers the OrderPlaced messages the typed
# example decodes.
resource "aws_sns_topic_subscription" "typed" {
  topic_arn            = aws_sns_topic.orders.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.typed.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.typed]
}

# Standard topic -> middleware queue. Fans the standard publishes out to the
# middleware example's queue as well.
resource "aws_sns_topic_subscription" "middleware" {
  topic_arn            = aws_sns_topic.standard.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.middleware.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.middleware]
}

# FIFO topic -> FIFO queue. Raw delivery preserves the MessageGroupId so ordered
# consumption keeps working end to end.
resource "aws_sns_topic_subscription" "fifo" {
  topic_arn            = aws_sns_topic.fifo.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.fifo.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.fifo]
}
