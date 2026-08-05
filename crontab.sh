#!/usr/bin/env bash

# 1. Load variables from .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
  if [ -z "$PROJECT_PATH" ]; then
    echo "Error: PROJECT_PATH environment variable is not set." >&2
    exit 1
  fi
else
  echo "Error: .env file not found!" >&2
  exit 1
fi

mkdir -p logs

TEMP_CRON=$(mktemp)

# Unquoted EOF allows $PROJECT_PATH to expand
cat > "$TEMP_CRON" << EOF
0 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines return-factors > logs/return_factors.log 2>&1
0 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines covariance-matrix > logs/covariance_matrix.log 2>&1
0 2 * * * cd $PROJECT_PATH && .venv/bin/python -m pipelines historical-data > logs/historical_data.log 2>&1
EOF

crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "Crontab updated. Monitor with: tail -f logs/*.log"