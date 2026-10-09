#!/usr/bin/env bash
# Textbook reliability cases against a running Fineract (savings deposits/withdrawals). Prints PASS/FINDING per case, nothing is assumed.
#   bash scripts/probe-fineract-reliability.sh [accountA] [accountB]
A=${1:-4}; B=${2:-5}
BASE=http://localhost:8443/fineract-provider/api/v1
H=(-s -u mifos:password -H "Fineract-Platform-TenantId: default" -H "Content-Type: application/json")
DATE=$(date -d yesterday '+%d %B %Y')
bal() { curl "${H[@]}" "$BASE/savingsaccounts/$1" | sed -E 's/.*"accountBalance":([-0-9.]+).*/\1/'; }
body() { echo "{\"transactionDate\":\"$DATE\",\"transactionAmount\":$1,\"paymentTypeId\":4,\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"}"; }
post() { # acct cmd amount [key] -> prints http code
  local extra=(); [ -n "$4" ] && extra=(-H "Idempotency-Key: $4")
  curl "${H[@]}" "${extra[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$1/transactions?command=$2" -d "$(body $3)"; }
calc() { awk "BEGIN{printf \"%g\", $1}"; }
RES=/tmp/rel-$$; mkdir -p $RES
verdict() { echo "$1: $2"; }

echo "accounts $A / $B start balance: $(bal $A) / $(bal $B)"; echo

# 1. identical deposits (same amount, same moment), DIFFERENT keys, same account: both must post
b0=$(bal $A); for i in 1 2; do (post $A deposit 7 "same-$$-$i" > $RES/1.$i) & done; wait
b1=$(bal $A); verdict "1 two identical deposits, different keys" "codes $(cat $RES/1.1) $(cat $RES/1.2), delta $(calc "$b1-$b0") (expect 14)"

# 2. same amount on two DIFFERENT accounts at once, different keys: each +amount
a0=$(bal $A); c0=$(bal $B); (post $A deposit 3 "x-$$-a" >$RES/2.a) & (post $B deposit 3 "x-$$-b" >$RES/2.b) & wait
verdict "2 same amount, two accounts" "codes $(cat $RES/2.a) $(cat $RES/2.b), deltas $(calc "$(bal $A)-$a0") / $(calc "$(bal $B)-$c0") (expect 3 / 3)"

# 3. 20 concurrent deposits of 1 on one account, no key: balance delta must equal number of 200 answers
b0=$(bal $A); for i in $(seq 1 20); do (post $A deposit 1 > $RES/3.$i) & done; wait
ok=$(cat $RES/3.* | grep -c '^200$'); verdict "3 20 concurrent deposits, no key" "200s=$ok, others=$(cat $RES/3.* | grep -v '^200$' | sort | uniq -c | tr '\n' ' '), delta $(calc "$(bal $A)-$b0")  (consistent if delta == 200s)"

# 4. SAME key sent 10 times at once: exactly one posting
b0=$(bal $A); for i in $(seq 1 10); do (post $A deposit 5 "dup-$$" > $RES/4.$i) & done; wait
verdict "4 same key x10 concurrently" "codes $(cat $RES/4.* | sort | uniq -c | tr '\n' ' '), delta $(calc "$(bal $A)-$b0") (expect 5)"

# 5. double spend: balance X, two concurrent withdrawals of 0.75*X: at most one may succeed, never overdraw
bal_now=$(bal $B); w=$(calc "$bal_now*0.75/1"); b0=$bal_now
(post $B withdrawal $w > $RES/5.1) & (post $B withdrawal $w > $RES/5.2) & wait
verdict "5 concurrent withdrawals of 75% of balance" "codes $(cat $RES/5.1) $(cat $RES/5.2), balance $b0 -> $(bal $B) (must stay >= 0)"
post $B deposit $w >/dev/null   # put it back if one succeeded

# 6. withdrawal larger than balance: clean 4xx, not 5xx, balance unchanged
b0=$(bal $B); c=$(post $B withdrawal 99999999); verdict "6 overdraft withdrawal" "code $c (expect 4xx, not 5xx), balance $b0 -> $(bal $B)"

# 7. amount edge cases: precision, zero, negative, huge, text
for amt in 0.1 0.005 0 -5 1e15 999999999999999999 '"abc"' null; do
  b0=$(bal $A); c=$(post $A deposit "$amt"); verdict "7 amount=$amt" "code $c, delta $(calc "$(bal $A)-$b0")"; done

# 8. missing / wrong resources and bad input: expect 4xx, never 5xx
verdict "8a deposit to missing account" "code $(post 99999999 deposit 1)"
verdict "8b unknown command" "code $(post $A bogus 1)"
verdict "8c malformed JSON" "code $(curl "${H[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d '{"transactionAmount":')"
verdict "8d empty body" "code $(curl "${H[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d '')"
verdict "8e wrong tenant" "code $(curl -s -u mifos:password -H 'Fineract-Platform-TenantId: nope' -o /dev/null -w '%{http_code}' $BASE/savingsaccounts/$A)"
verdict "8f no auth" "code $(curl -s -H 'Fineract-Platform-TenantId: default' -o /dev/null -w '%{http_code}' $BASE/savingsaccounts/$A)"

# 9. two DIFFERENT people with identical name + date of birth: are both created?
for i in 1 2; do (curl "${H[@]}" -o $RES/9.$i -w '%{http_code}' -X POST $BASE/clients -d "{\"officeId\":1,\"legalFormId\":1,\"firstname\":\"Same\",\"lastname\":\"Person$$\",\"dateOfBirth\":\"01 January 1990\",\"active\":false,\"locale\":\"en\",\"dateFormat\":\"dd MMMM yyyy\",\"submittedOnDate\":\"$DATE\"}" > $RES/9c.$i) & done; wait
verdict "9 two clients, identical name and birth date" "codes $(cat $RES/9c.1) $(cat $RES/9c.2) (200 200 = both allowed, they get different ids: $(grep -o '"clientId":[0-9]*' $RES/9.1 $RES/9.2 | tr '\n' ' '))"

# 10. Fineract still healthy after all of the above
verdict "10 health" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8443/fineract-provider/actuator/health)"
echo; echo "end balance $(bal $A) / $(bal $B)"; rm -rf $RES
