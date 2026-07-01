#!/bin/bash
echo "=== GCP CPU Quota Check ==="
gcloud compute project-info describe \
  --format="json(quotas)" | python3 -c "
import json, sys
quotas = json.load(sys.stdin)['quotas']
for q in quotas:
    if 'CPU' in q['metric']:
        limit = q['limit']
        used = q['usage']
        remaining = limit - used
        status = 'OK' if remaining >= 4 else 'WARNING — do not apply'
        print(f'{q[\"metric\"]} → limit: {limit} / used: {used} / remaining: {remaining} [{status}]')
"
