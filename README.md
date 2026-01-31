# sf-data-pipelines-fulton
Data pipelines built by the Silver Fund Developer team that run on the Fulton Super Computer.

# factor exposures and covariance matrix
- uploaded daily to S3 buckets by cron job on login node of supercomputer.
- update the crontab with the crontab.sh script

run the script manually with (same as in crontab)
.venv/bin/python -m pipelines covariance-matrix
