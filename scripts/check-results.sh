#!/bin/bash
# Query BigQuery for current experiment results

bq query --project_id green-cloud-pipeline-v2 \
  --use_legacy_sql=false \
  'SELECT
     admission_mode,
     scaling_mode,
     processed_region,
     origin_region,
     was_redirected,
     COUNT(*) as jobs,
     ROUND(AVG(latency_ms), 1) as avg_latency_ms,
     ROUND(MIN(latency_ms), 1) as min_latency_ms,
     ROUND(MAX(latency_ms), 1) as max_latency_ms,
     ROUND(SUM(energy_kwh), 6) as total_energy_kwh,
     ROUND(SUM(carbon_emitted_g), 4) as total_carbon_g,
     ROUND(SUM(carbon_saved_g), 4) as total_carbon_saved_g,
     COUNTIF(sla_violated) as sla_violations
   FROM green_cloud_pipeline.jobs
   GROUP BY admission_mode, scaling_mode, processed_region,
            origin_region, was_redirected
   ORDER BY admission_mode, processed_region, origin_region'
