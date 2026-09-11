#!/usr/bin/env bash

set -o pipefail

# Find absolute path to current directory if running from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [ -f "$ENV_FILE" ]; then
  # Sourcing .env natively in Bash allows nested variable references like ${USERNAME} to resolve
  set -a
  source "$ENV_FILE"
  set +a
  
  if [ -z "$PROJECT_PATH" ]; then
    echo "Error: PROJECT_PATH environment variable is not set." >&2
    exit 1
  fi
else
  echo "Error: .env file not found at $ENV_FILE!" >&2
  exit 1
fi

PIPELINE_NAME="$1" # First value supplied by the wrapper such as "return-factors", "covariance-matrix", or "historical-data"
LOG_FILE="$2" # Second value supplied by the wrapper, e.g., "return_factors.log", "covariance_matrix.log", or "historical_data.log" 

# Ensure log directory exists
if [ -z "$PIPELINE_NAME" ] || [ -z "$LOG_FILE" ]; then
    echo "Usage: ./run_pipeline_with_alerts.sh PIPELINE_NAME LOG_FILE" >&2
    exit 1
fi

# Ensure the directory for the pipeline log exists
mkdir -p "$(dirname "$LOG_FILE")"

# Run the pipeline, save its output, and capture whether it succeeded
"$SCRIPT_DIR/.venv/bin/python" -m pipelines "$PIPELINE_NAME" \
    > "$LOG_FILE" 2>&1
EXIT_CODE=$?

# If the pipeline fails, collects the last 20 lines of the log
if [ "$EXIT_CODE" -ne 0 ]; then 
    ERROR_DETAILS="$(tail -n 20 "$LOG_FILE")"
    
    # Checks if Slack env variables are connected
    if [ -z "$SLACK_BOT_TOKEN" ] || [ -z "$SLACK_CHANNEL_ID" ]; then
        echo "Error: Slack environment variables are not set. Cannot send alert." >&2
        exit "$EXIT_CODE"
        
    fi

    # Build the Slack Message
    printf -v SLACK_MESSAGE \
    'Fulton pipeline failed\n*Pipeline:* `%s`\n*Server:* `%s`\n*Exit code:* `%s`\n*Recent log output:*\n```%s```' \
    "$PIPELINE_NAME" \
    "$(hostname)" \
    "$EXIT_CODE" \
    "$ERROR_DETAILS"

    # Send the Slack message using curl
    SLACK_RESPONSE="$(curl -sS \
        -X POST "https://slack.com/api/chat.postMessage" \
        -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
        --data-urlencode "channel=$SLACK_CHANNEL_ID" \
        --data-urlencode "text=$SLACK_MESSAGE")"
        
    # Check if slack reject the notification
    if [[ "$SLACK_RESPONSE" != *'"ok":true'* ]]; then
        echo "Slack notification failed: $SLACK_RESPONSE" >&2
    fi
fi

#return the same result as the original pipeline
exit "$EXIT_CODE"
