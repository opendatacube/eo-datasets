resource "aws_sqs_queue" "indexing_queue" {
  name                      = "indexing_queue"
  delay_seconds             = 90
  max_message_size          = 2048
  message_retention_seconds = 86400
  receive_wait_time_seconds = 10
  redrive_policy            = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.indexing_queue_deadletter.arn}\",\"maxReceiveCount\":2}"

  tags = {
    Environment = "production"
  }
}

resource "aws_sqs_queue" "indexing_queue_deadletter" {
  name  = "indexing_queue_deadletter"
}

data "aws_caller_identity" "current" {}

resource "aws_sqs_queue_policy" "account_wide_access" {
  queue_url = "${aws_sqs_queue.indexing_queue.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "arn:aws:sqs:us-west-2:${data.aws_caller_identity.current.account_id}:collection-2-nigeria/SQSDefaultPolicy",
  "Statement": [
    {
      "Sid": "Sid1568097085260",
      "Effect": "Allow",
      "Principal": {
        "AWS": ["${data.aws_caller_identity.current.account_id}"]
      },
      "Action": "SQS:*",
      "Resource": "${aws_sqs_queue.indexing_queue.arn}"
    }
  ]
}
POLICY
}