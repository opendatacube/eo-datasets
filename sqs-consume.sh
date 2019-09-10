#!/usr/bin/env bash

MSG=$(aws sqs receive-message --queue-url $QUEUE_URL);

[[ -z $MSG ]] && exit

RECEIPT_HANDLE=$(echo "$MSG" | jq -r '.Messages[] | .ReceiptHandle');
MESSAGE_BODY=$(echo "$MSG" | jq -r '.Messages[] | .Body');

eo3-prepare usgs-col2 --output-base ${OUTPUT_BASE} ${MESSAGE_BODY} && aws sqs delete-message --queue-url ${QUEUE_URL} --receipt-handle ${RECEIPT_HANDLE}
