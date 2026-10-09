#!/usr/bin/env bash
# Retry-jitter experiment: same deposit load, JITTER=0 (today) vs JITTER=0.5. Usage: perf/retry-jitter-bench.sh <jitter> [rate] [duration]
set -e
J=${1:-0}; RATE=${2:-50}; DUR=${3:-60s}
export MAX_BATCHES=1 PARALLEL=false EVENT_LOG_LEVEL=INFO JITTER=$J
docker compose -f docker-compose.yml -f docker-compose.infra.yml -f perf/docker-compose.events-bench.yml -f perf/docker-compose.events-drain.yml up -d --force-recreate fineract >/dev/null 2>&1
until docker logs finract_jenny-fineract-1 2>&1 | grep -q "Started ServerApplication"; do sleep 5; done
sleep 20
echo "== jitter=$J rate=$RATE duration=$DUR"
MSYS_NO_PATHCONV=1 docker run --rm -i -e RATE=$RATE -e DURATION=$DUR -e FINERACT_URL=http://host.docker.internal:8443/fineract-provider/api/v1 \
  -v "$(pwd -W)/perf:/perf" grafana/k6 run --summary-export=/perf/jitter-$J.json /perf/fineract-deposits.js 2>&1 | grep -E "http_req_failed|http_req_duration|http_reqs|checks|iterations" 
