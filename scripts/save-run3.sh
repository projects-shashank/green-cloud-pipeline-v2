#!/bin/bash
echo "=== Saving Run 3 (Green + Predictive) results ==="
bq query --project_id green-cloud-pipeline-v2 \
  --use_legacy_sql=false \
  'CREATE OR REPLACE TABLE green_cloud_pipeline.run3_green_predictive AS
   SELECT * FROM green_cloud_pipeline.jobs'
echo "Saved to: green_cloud_pipeline.run3_green_predictive"
bq query --project_id green-cloud-pipeline-v2 \
  --use_legacy_sql=false \
  'SELECT COUNT(*) as total_jobs FROM green_cloud_pipeline.run3_green_predictive'
