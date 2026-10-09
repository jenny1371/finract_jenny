// Drives deposits straight into Fineract to generate external events (one SavingsDepositBusinessEvent per deposit).
// Used by the event-throughput benchmark; see perf/README.md.
//
//   docker run --rm -i -e RATE=50 -e DURATION=60s -e FINERACT_URL=http://host.docker.internal:8443/fineract-provider/api/v1 \
//     -v "<repo>/perf:/perf" grafana/k6 run /perf/fineract-deposits.js
//
// Deposits are spread over savings accounts FIRST..LAST (default 3..12, the load-test merchants) so the optimistic
// lock on a single account does not become the thing being measured.
import http from 'k6/http';
import { check } from 'k6';
import encoding from 'k6/encoding';

const BASE = __ENV.FINERACT_URL || 'http://localhost:8443/fineract-provider/api/v1';
const RATE = parseInt(__ENV.RATE || '50');
const DURATION = __ENV.DURATION || '60s';
const FIRST = parseInt(__ENV.FIRST || '3');
const LAST = parseInt(__ENV.LAST || '12');
const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];

const y = new Date(Date.now() - 86400000);                       // yesterday: never "in the future" for Fineract
const DATE = `${String(y.getUTCDate()).padStart(2, '0')} ${MONTHS[y.getUTCMonth()]} ${y.getUTCFullYear()}`;
const AUTH = 'Basic ' + encoding.b64encode('mifos:password');

export const options = {
  scenarios: { steady: { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: 50, maxVUs: 300 } },
  thresholds: { http_req_failed: ['rate<0.02'] },
};

export default function () {
  const account = FIRST + (__ITER % (LAST - FIRST + 1));
  const res = http.post(`${BASE}/savingsaccounts/${account}/transactions?command=deposit`,
    JSON.stringify({ transactionDate: DATE, transactionAmount: 1, paymentTypeId: 4, dateFormat: 'dd MMMM yyyy', locale: 'en' }),
    { headers: { 'Content-Type': 'application/json', Authorization: AUTH, 'Fineract-Platform-TenantId': 'default' } });
  check(res, { 'deposit ok (200)': (r) => r.status === 200 });
}
