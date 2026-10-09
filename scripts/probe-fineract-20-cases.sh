#!/usr/bin/env bash
# 20 distinct failure scenarios against a running Fineract, each with an explicit expectation. 5xx or a hang (000) = FINDING.
#   bash scripts/probe-fineract-20-cases.sh [accountA=4] [accountB=5]
A=${1:-4}; B=${2:-5}; CA=5; CB=6   # client ids owning accounts A and B
BASE=http://localhost:8443/fineract-provider/api/v1
AUTH=(-u mifos:password -H "Fineract-Platform-TenantId: default")
YDAY=$(date -d yesterday '+%d %B %Y'); T=/tmp/p20-$$; mkdir -p $T; FIND=0
calc() { awk "BEGIN{printf \"%g\", $1}"; }
bal() { curl -s "${AUTH[@]}" $BASE/savingsaccounts/$1 | sed -E 's/.*"accountBalance":([-0-9.]+).*/\1/'; }
tx() { echo "{\"transactionDate\":\"${4:-$YDAY}\",\"transactionAmount\":$3,\"paymentTypeId\":4,\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"${5:+,$5}}"; }
post() { # acct cmd amount [date] [extra json] [idem key] -> http code (body in $T/body)
  local k=(); [ -n "$6" ] && k=(-H "Idempotency-Key: $6")
  curl -s -m 60 "${AUTH[@]}" -H "Content-Type: application/json" "${k[@]}" -o $T/body -w '%{http_code}' -X POST "$BASE/savingsaccounts/$1/transactions?command=$2" -d "$(tx $1 $2 "$3" "$4" "$5")"; }
v() { local flag=""; if echo "$2" | grep -Eq '(^|[ :=\[])(5[0-9][0-9]|000)([ ,;)\]]|$)'; then flag="  <== FINDING (5xx/hang)"; FIND=$((FIND+1)); fi; echo "$1: $2$flag"; }

echo "start balances $(bal $A) / $(bal $B)"; echo
# 1 impossible date
c=$(post $A deposit 1 "29 February 2027"); v "01 impossible date 29 Feb 2027" "code $c (expect 400)"
# 2 date does not match dateFormat
c=$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d '{"transactionDate":"05 October 2026","transactionAmount":1,"paymentTypeId":4,"dateFormat":"dd/MM/yyyy","locale":"en"}'); v "02 date does not match dateFormat" "code $c (expect 400)"
# 3 before activation, 4 future
b0=$(bal $A); c=$(post $A deposit 1 "31 December 2025"); v "03 before account activation" "code $c, delta $(calc "$(bal $A)-$b0") (expect 4xx, 0)"
b0=$(bal $A); c=$(post $A deposit 1 "01 January 2099"); v "04 future date" "code $c, delta $(calc "$(bal $A)-$b0") (expect 4xx, 0)"
# 5 account that is not active (pending approval)
n=$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -X POST $BASE/savingsaccounts -d "{\"clientId\":$CA,\"productId\":1,\"locale\":\"en\",\"dateFormat\":\"dd MMMM yyyy\",\"submittedOnDate\":\"$YDAY\"}" | grep -o '"savingsId":[0-9]*' | head -1 | cut -d: -f2)
c=$(post "$n" deposit 1); v "05 deposit into a not-yet-approved account (id $n)" "code $c (expect 4xx)"
# 6 undo the same transaction twice
u() { curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -o $T/ubody -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions/$1?command=undo" -d '{}'; }
post $A deposit 1 "" "" "u6-$$" >/dev/null; id=$(grep -o '"resourceId":[0-9]*' $T/body | cut -d: -f2); b0=$(bal $A)
c1=$(u $id); c2=$(u $id); v "06 undo the same transaction twice" "codes $c1 then $c2, delta $(calc "$(bal $A)-$b0") (expect 200 then 4xx, -1 once)"
# 7 undo concurrently
post $A deposit 1 "" "" "u7-$$" >/dev/null; id=$(grep -o '"resourceId":[0-9]*' $T/body | cut -d: -f2); b0=$(bal $A)
for i in 1 2 3 4 5; do (u $id > $T/u7.$i) & done; wait
v "07 undo the same transaction 5x concurrently" "codes $(for f in $T/u7.*; do echo -n "$(cat $f) "; done), delta $(calc "$(bal $A)-$b0") (expect -1 exactly, no 5xx)"
# 8 withdraw the whole balance, then one cent more
orig=$(bal $B); c1=$(post $B withdrawal $orig); mid=$(bal $B); c2=$(post $B withdrawal 0.01); end=$(bal $B); post $B deposit $orig >/dev/null
v "08 withdraw exact balance then 0.01 more" "codes $c1 $c2, balance $orig -> $mid -> $end (expect 200, 403, 0 then 0)"
# 9 hostile strings in externalId
EMOJI=$(printf '\xF0\x9F\x98\x80 \xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E')
LONG=$(head -c 5000 /dev/zero | tr '\0' 'x')
: > $T/c9
for s in "'; DROP TABLE m_client;--" "<script>alert(1)</script>" "$LONG" "$EMOJI" '%s%n%x'; do
  b0=$(bal $A); c=$(post $A deposit 1 "" "\"externalId\":\"$s\""); d=$(calc "$(bal $A)-$b0"); echo "   9 externalId len ${#s}: code $c delta $d"; echo "$c" >> $T/c9; done
v "09 hostile strings in externalId" "codes $(sort $T/c9 | uniq -c | tr '\n' ';') (any 5xx = finding; a 200 has delta 1)"
# 10 odd idempotency keys
K255=$(head -c 255 /dev/zero | tr '\0' 'k'); K256=$(head -c 256 /dev/zero | tr '\0' 'k'); CYR=$(printf '\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87-\xE6\x97\xA5')
: > $T/c10
for k in "$K255" "$K256" " " "key with spaces" "$CYR" "a,b;c=d"; do
  b0=$(bal $A); c1=$(post $A deposit 1 "" "" "$k"); c2=$(post $A deposit 1 "" "" "$k"); echo "   10 key len ${#k}: $c1 $c2 delta $(calc "$(bal $A)-$b0")"; echo -n "$c1/$c2 " >> $T/c10; done
v "10 odd idempotency keys" "codes $(cat $T/c10) (each key may post at most once)"
# 11 JSON oddities
b0=$(bal $A); c1=$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d "{\"transactionDate\":\"$YDAY\",\"transactionAmount\":1,\"transactionAmount\":1000,\"paymentTypeId\":4,\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"}"); d1=$(calc "$(bal $A)-$b0")
c2=$(post $A deposit 1 "" '"unknownField":123'); c3=$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d "[1,2,3]")
c4=$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d "{\"transactionDate\":\"$YDAY\",\"transactionAmount\":\"5\",\"paymentTypeId\":4,\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"}")
v "11 JSON oddities" "duplicate key: code $c1 (delta $d1: 1 or 1000), unknown field: code $c2, array body: code $c3, amount as string: code $c4 (expect no 5xx)"
# 12 wrong content type
: > $T/c12; for ct in "text/plain" "application/xml" "application/x-www-form-urlencoded"; do echo -n "$(curl -s -m 30 "${AUTH[@]}" -H "Content-Type: $ct" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d "$(tx $A deposit 1)") " >> $T/c12; done
v "12 wrong Content-Type (text, xml, form)" "codes $(cat $T/c12)(expect 4xx)"
# 13 oversized body (3 MB)
head -c 3000000 /dev/zero | tr '\0' 'a' > $T/big
{ printf '{"transactionDate":"%s","transactionAmount":1,"paymentTypeId":4,"dateFormat":"dd MMMM yyyy","locale":"en","externalId":"' "$YDAY"; cat $T/big; printf '"}'; } > $T/bigbody
c=$(curl -s -m 60 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" --data-binary @$T/bigbody)
v "13 3 MB request body" "code $c, health after: $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8443/fineract-provider/actuator/health)"
# 14 wrong HTTP methods
: > $T/c14; for m in GET PUT PATCH DELETE; do echo -n "$m:$(curl -s -m 30 "${AUTH[@]}" -H 'Content-Type: application/json' -o /dev/null -w '%{http_code}' -X $m "$BASE/savingsaccounts/$A/transactions?command=deposit" -d '{}') " >> $T/c14; done
v "14 wrong HTTP methods on the deposit endpoint" "$(cat $T/c14)(expect 4xx/405)"
# 15 pagination / sort abuse
: > $T/c15; for q in "limit=-1" "limit=0" "offset=-5&limit=5" "limit=100000000" "orderBy=bogus;drop" "sortOrder=sideways" "limit=abc" "offset=99999999999999999999"; do
  echo -n "[$q]=$(curl -s -m 60 "${AUTH[@]}" -o /dev/null -w '%{http_code}' "$BASE/clients?$q") " >> $T/c15; done
v "15 pagination and sort abuse" "$(cat $T/c15)(expect no 5xx)"
# 16 weird path ids
: > $T/c16; for p in abc -1 0 9223372036854775808 "1%00" "1;x" 99999999999; do echo -n "[$p]=$(curl -s -m 30 "${AUTH[@]}" -o /dev/null -w '%{http_code}' "$BASE/savingsaccounts/$p") " >> $T/c16; done
v "16 weird path ids" "$(cat $T/c16)(expect 4xx)"
# 17 three clients created at once with the same externalId
x="ext-$$-$RANDOM"; mk() { curl -s -m 60 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST $BASE/clients -d "{\"officeId\":1,\"legalFormId\":1,\"firstname\":\"Dup\",\"lastname\":\"Ext\",\"externalId\":\"$x\",\"active\":false,\"locale\":\"en\",\"dateFormat\":\"dd MMMM yyyy\",\"submittedOnDate\":\"$YDAY\"}"; }
for i in 1 2 3; do (mk > $T/17.$i) & done; wait
rows=$(docker exec finract_jenny-fineract-db-1 psql -U postgres -d fineract_default -t -A -c "select count(*) from m_client where external_id='$x'")
v "17 3 concurrent clients, same externalId" "codes $(for f in $T/17.*; do echo -n "$(cat $f) "; done) rows=$rows (expect exactly one 200, rows=1, no 5xx)"
# 18 crossing transfers A<->B concurrently
xfer() { curl -s -m 90 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}' -X POST $BASE/accounttransfers -d "{\"fromOfficeId\":1,\"fromClientId\":$3,\"fromAccountType\":2,\"fromAccountId\":$1,\"toOfficeId\":1,\"toClientId\":$4,\"toAccountType\":2,\"toAccountId\":$2,\"transferDate\":\"$YDAY\",\"transferAmount\":1,\"transferDescription\":\"p20\",\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"}"; }
s0=$(calc "$(bal $A)+$(bal $B)"); for i in $(seq 1 10); do (xfer $A $B $CA $CB > $T/18a.$i) & (xfer $B $A $CB $CA > $T/18b.$i) & done; wait
s1=$(calc "$(bal $A)+$(bal $B)"); v "18 20 crossing transfers A<->B" "codes $(cat $T/18a.* $T/18b.* | sed 's/\([0-9]\{3\}\)/\1\n/g' | sort | uniq -c | tr '\n' ' ') total $s0 -> $s1 (money conserved? no 5xx?)"
# 19 mixed concurrent ops: balance == start + deposits - withdrawals that answered 200
b0=$(bal $A); for i in $(seq 1 15); do (post $A deposit 2 "" "" "m19d-$$-$i" > $T/19d.$i) & (post $A withdrawal 1 "" "" "m19w-$$-$i" > $T/19w.$i) & done; wait
dd=$(cat $T/19d.* | sed 's/\([0-9]\{3\}\)/\1\n/g' | grep -c '^200$'); ww=$(cat $T/19w.* | sed 's/\([0-9]\{3\}\)/\1\n/g' | grep -c '^200$'); exp=$(calc "2*$dd-$ww"); got=$(calc "$(bal $A)-$b0")
v "19 15 deposits(2) + 15 withdrawals(1) concurrently" "200s: $dd deposits, $ww withdrawals, expected delta $exp, actual $got, other codes: $(cat $T/19d.* $T/19w.* | sed 's/\([0-9]\{3\}\)/\1\n/g' | grep -v '^200$' | grep . | sort | uniq -c | tr '\n' ' ')"
# 20 client gives up (timeout), then retries with the same key: exactly one posting each
b0=$(bal $A); : > $T/c20; for i in 1 2 3; do k="abort-$$-$i"; curl -s -m 0.05 "${AUTH[@]}" -H "Content-Type: application/json" -H "Idempotency-Key: $k" -o /dev/null -X POST "$BASE/savingsaccounts/$A/transactions?command=deposit" -d "$(tx $A deposit 1)"; sleep 2; echo -n "$(post $A deposit 1 "" "" "$k") " >> $T/c20; done
v "20 client aborts, retries with same key (3 rounds)" "retry codes $(cat $T/c20), delta $(calc "$(bal $A)-$b0") (expect 3: each posted once)"
echo; echo "FINDINGS (5xx/hang): $FIND   health: $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8443/fineract-provider/actuator/health)   end balances $(bal $A) / $(bal $B)"; rm -rf $T
