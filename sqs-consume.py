#!/usr/bin/env python3
"""
Inspired by amqp-consume
"""
import argparse
import boto3
import subprocess
import os

parser = argparse.ArgumentParser()
parser.add_argument(
    "--message-timeout",
    type=int,
    default=30,
    help="How long should messages take to process?",
)
parser.add_argument(
    "-q",
    "--queue-url",
    default=os.environ.get("QUEUE_URL", None),
    help="Defaults to environment variable QUEUE_URL",
)
parser.add_argument("command", nargs="+", help="The command to use to process messages")

args = parser.parse_args()

print(f"Receiving messages from {args.queue_url}")
# Get the service resource
sqs = boto3.resource("sqs")

# Create the queue. This returns an SQS.Queue instance
queue = sqs.Queue(args.queue_url)


while True:
    messages = queue.receive_messages(
        VisibilityTimeout=args.message_timeout, MaxNumberOfMessages=1
    )

    if len(messages) == 0:
        print("No messages to process. Exiting.")
        break

    for message in messages:
        try:
            command = args.command.copy()
            command.append(message.body)
            subprocess.check_call(command)
            message.delete()
        except subprocess.CalledProcessError:
            print("Failed to process")
