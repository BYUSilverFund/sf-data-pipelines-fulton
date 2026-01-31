#!/bin/bash

mkdir -p logs

TEMP_CRON=$(mktemp)

cat > "$TEMP_CRON" << 'EOF'
0 2 * * * cd /home/benja100/projects/sf-data-pipelines-fulton && .venv/bin/python -m pipelines return-factors > logs/return_factors.log 2>&1
0 2 * * * cd /home/benja100/projects/sf-data-pipelines-fulton && .venv/bin/python -m pipelines covariance-matrix > logs/covariance_matrix.log 2>&1
EOF

crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "Crontab updated. Monitor with: tail -f logs/covariance_matrix.log"