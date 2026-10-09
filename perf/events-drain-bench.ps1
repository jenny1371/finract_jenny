# Event-drain benchmark for FINERACT-2900. Creates a backlog of TO_BE_SENT events and records how fast the
# "Send Asynchronous Events" job drains it. Run once per MAX_BATCHES setting (see docker-compose.events-drain.yml).
#   powershell -File perf/events-drain-bench.ps1 -Label control -MaxSeconds 150
param([string]$Label = 'run', [int]$MaxSeconds = 150, [int]$Backlog = 12000)
$db = 'finract_jenny-fineract-db-1'
function Sql($q) { docker exec $db psql -U postgres -d fineract_default -t -A -c $q }

# park everything that is pending (UPDATE, not DELETE), then create the benchmark backlog from real rows
Sql "update m_external_event set status='SENT', sent_at=now() where status='TO_BE_SENT'" | Out-Null
$copies = [math]::Ceiling($Backlog / [int](Sql "select count(*) from m_external_event where type='SavingsDepositBusinessEvent'"))
Sql "insert into m_external_event (type, created_at, status, business_date, data, idempotency_key, schema, category, aggregate_root_id) select e.type, now(), 'TO_BE_SENT', e.business_date, e.data, gen_random_uuid()::text, e.schema, e.category, e.aggregate_root_id from m_external_event e cross join generate_series(1,$copies) g where e.type='SavingsDepositBusinessEvent' limit $Backlog" | Out-Null
$start = [int](Sql "select count(*) from m_external_event where status='TO_BE_SENT'")
"[$Label] backlog created: $start events; sampling every 1 s for up to $MaxSeconds s (batch size = $(Sql "select value from c_configuration where name='external-event-batch-size'"))"

$t0 = Get-Date; $prev = $start; $steps = @()
while ($true) {
  Start-Sleep -Seconds 1
  $n = [int](Sql "select count(*) from m_external_event where status='TO_BE_SENT'")
  $elapsed = ((Get-Date) - $t0).TotalSeconds
  if ($n -ne $prev) { $steps += [pscustomobject]@{ At = [math]::Round($elapsed, 0); Sent = $prev - $n; Left = $n }; "{0,5}s  sent {1,6}  left {2,6}" -f [math]::Round($elapsed, 0), ($prev - $n), $n }
  $prev = $n
  if ($n -eq 0 -or $elapsed -ge $MaxSeconds) { break }
}
"[$Label] result: {0} of {1} events sent in {2:N0} s ({3} job runs observed)" -f ($start - $prev), $start, ((Get-Date) - $t0).TotalSeconds, $steps.Count
