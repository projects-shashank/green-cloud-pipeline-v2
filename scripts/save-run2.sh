#!/bin/bash
echo "=== Saving Run 2 (Green + Reactive) results ==="
bq query --project_id green-cloud-pipeline-v2 \
  --use_legacy_sql=false \
  'CREATE OR REPLACE TABLE green_cloud_pipeline.run2_green_reactive AS
   SELECT * FROM green_cloud_pipeline.jobs'
echo "Saved to: green_cloud_pipeline.run2_green_reactive"
bq query --project_id green-cloud-pipeline-v2 \
  --use_legacy_sql=false \
  'SELECT COUNT(*) as total_jobs FROM green_cloud_pipeline.run2_green_reactive'
