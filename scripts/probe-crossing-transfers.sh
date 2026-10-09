#!/usr/bin/env bash
# Crossing transfers A->B and B->A at the same time. Reports HTTP codes, DB deadlocks (pg_stat_database) and conservation of money.
#   bash scripts/probe-crossing-transfers.sh [rounds=3] [perDirection=10] [accountA=4] [accountB=5]
ROUNDS=${1:-3}; N=${2:-10}; A=${3:-4}; B=${4:-5}; CA=5; CB=6
BASE=http://localhost:8443/fineract-provider/api/v1; AUTH=(-u mifos:password -H "Fineract-Platform-TenantId: default")
YDAY=$(date -d yesterday '+%d %B %Y'); T=/tmp/ct-$$; mkdir -p $T
bal() { curl -s "${AUTH[@]}" $BASE/savingsaccounts/$1 | sed -E 's/.*"accountBalance":([-0-9.]+).*/\1/'; }
dl() { docker exec finract_jenny-fineract-db-1 psql -U postgres -d fineract_default -t -A -c "select deadlocks from pg_stat_database where datname='fineract_default'"; }
xfer() { curl -s -m 120 "${AUTH[@]}" -H "Content-Type: application/json" -o /dev/null -w '%{http_code}\n' -X POST $BASE/accounttransfers -d "{\"fromOfficeId\":1,\"fromClientId\":$3,\"fromAccountType\":2,\"fromAccountId\":$1,\"toOfficeId\":1,\"toClientId\":$4,\"toAccountType\":2,\"toAccountId\":$2,\"transferDate\":\"$YDAY\",\"transferAmount\":1,\"transferDescription\":\"ct\",\"dateFormat\":\"dd MMMM yyyy\",\"locale\":\"en\"}"; }
tot_ok=0; tot_all=0; d0=$(dl)
for r in $(seq 1 $ROUNDS); do
  s0=$(awk "BEGIN{print $(bal $A)+$(bal $B)}"); a0=$(bal $A); t0=$(date +%s.%N)
  for i in $(seq 1 $N); do xfer $A $B $CA $CB > $T/f.$i & xfer $B $A $CB $CA > $T/b.$i & done; wait
  t1=$(date +%s.%N); ok=$(cat $T/f.* $T/b.* | grep -c '^200$'); all=$((2*N)); s1=$(awk "BEGIN{print $(bal $A)+$(bal $B)}")
  fw=$(cat $T/f.* | grep -c '^200$'); bw=$(cat $T/b.* | grep -c '^200$'); exp=$(awk "BEGIN{print $bw-$fw}"); got=$(awk "BEGIN{print $(bal $A)-$a0}")
  echo "round $r: $ok/$all ok, codes: $(cat $T/f.* $T/b.* | sort | uniq -c | tr '\n' ' ') | A delta $got (expected $exp) | total $s0 -> $s1 | $(awk "BEGIN{printf \"%.1f\", $t1-$t0}")s"
  tot_ok=$((tot_ok+ok)); tot_all=$((tot_all+all)); rm -f $T/*
done
echo "SUMMARY: $tot_ok/$tot_all transfers succeeded, DB deadlocks during the run: $(( $(dl) - d0 ))"; rm -rf $T
