// Load test: create orders at a steady rate and watch how latency and the saga behave.
//
//   docker run --rm -i -e RATE=10 -e DURATION=60s -e MERCHANTS=1 -e BASE_URL=http://host.docker.internal:8081 \
//     -v "<repo>/perf:/perf" grafana/k6 run /perf/order-flow.js
//   (or, with k6 installed:  k6 run perf/order-flow.js)
//
// Knobs (env): RATE (orders/s, default 20), DURATION (default 60s), BASE_URL,
//   MERCHANTS (distinct merchants; each needs its own Fineract account, see scripts/seed-load-merchants.ps1).
// With MERCHANTS=1 every order books into ONE Fineract account (worst case: our per-account lock serializes writes).
// This measures the API. For end-to-end time to COMPLETED run the SQL in perf/README.md afterwards.
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const RATE = parseInt(__ENV.RATE || '20');
const DURATION = __ENV.DURATION || '60s';
const BASE = __ENV.BASE_URL || 'http://localhost:8081';
const MERCHANTS = parseInt(__ENV.MERCHANTS || '1');

const createTime = new Trend('order_create_ms', true);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    order_create_ms: ['p(99)<500'],      // the API must stay fast: the saga does the slow work asynchronously
  },
};

export default function () {
  const headers = { 'Content-Type': 'application/json', 'Idempotency-Key': `${__VU}-${__ITER}-${Date.now()}` };
  const order = { amount: 1000, currency: 'USD' };
  if (MERCHANTS > 1) order.merchantId = `load-${__ITER % MERCHANTS}`;     // body, not header: that is what the saga uses
  const res = http.post(`${BASE}/orders`, JSON.stringify(order), { headers });
  createTime.add(res.timings.duration);
  check(res, { 'order accepted (201)': (r) => r.status === 201 });
}
