// k6 load test skeleton: hammer the booking path, compare p99 before/after a fix.
//   k6 run perf/ledger-booking.js
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate', startRate: 5, timeUnit: '1s', preAllocatedVUs: 50,
      stages: [{ target: 50, duration: '1m' }, { target: 200, duration: '2m' }],
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(99)<500'] },
};

export default function () {
  const res = http.post('http://localhost:8081/orders',
    JSON.stringify({ amount: 1000, currency: 'USD' }),
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `${__VU}-${__ITER}` } });
  check(res, { accepted: (r) => r.status < 300 });
}
