#!/usr/bin/env bash

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

# Ensure log directory exists
mkdir -p "$PROJECT_PATH/logs"

# Get current crontab, stripping out old entries for these specific pipelines to avoid duplicates
EXISTING_CRON=$(crontab -l 2>/dev/null | grep -v "sf-data-pipelines-fulton" || true)

TEMP_CRON=$(mktemp)

# Write back existing jobs plus the updated pipeline jobs with fully expanded paths
cat > "$TEMP_CRON" << EOF
$EXISTING_CRON
0 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines return-factors > logs/return_factors.log 2>&1
1 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines covariance-matrix > logs/covariance_matrix.log 2>&1
2 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines historical-data > logs/historical_data.log 2>&1
EOF

# Remove empty lines if any were introduced
sed -i '/^$/d' "$TEMP_CRON"

crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "Crontab updated successfully. Monitor logs with: tail -f $PROJECT_PATH/logs/*.log. Inspect crontab with crontab -l"