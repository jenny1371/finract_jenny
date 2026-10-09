# Probe: does Fineract book a deposit once when the same Idempotency-Key is sent N times concurrently?
# Reusable evidence for design decisions and upstream bug reports.
#   powershell -File scripts/probe-fineract-idempotency.ps1 -AccountId 2 -Parallel 10
param(
  [int]$AccountId = 2,
  [int]$Parallel = 10,
  [string]$HeaderName = 'Idempotency-Key',
  [switch]$NoKey          # control run: no key at all (expect Parallel deposits)
)
$ErrorActionPreference = 'Stop'
$base = 'http://localhost:8443/fineract-provider/api/v1'
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('mifos:password'))
$key = [guid]::NewGuid().ToString()
$headers = @{ Authorization = $auth; 'Fineract-Platform-TenantId' = 'default' }

function Balance { (Invoke-RestMethod -Uri "$base/savingsaccounts/$AccountId" -Headers $headers).summary.accountBalance }
function Txns { @((Invoke-RestMethod -Uri "$base/savingsaccounts/$AccountId`?associations=transactions" -Headers $headers).transactions).Count }

$before = Balance; $txBefore = Txns
$body = '{"transactionDate":"01 January 2026","transactionAmount":1,"paymentTypeId":4,"dateFormat":"dd MMMM yyyy","locale":"en"}'

$jobs = 1..$Parallel | ForEach-Object {
  Start-Job -ArgumentList $base, $AccountId, $auth, $key, $HeaderName, $NoKey.IsPresent, $body -ScriptBlock {
    param($base, $acct, $auth, $key, $hn, $nokey, $body)
    $h = @{ Authorization = $auth; 'Fineract-Platform-TenantId' = 'default' }
    if (-not $nokey) { $h[$hn] = $key }
    try {
      $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$base/savingsaccounts/$acct/transactions?command=deposit" `
        -Headers $h -ContentType 'application/json' -Body $body
      [int]$r.StatusCode
    } catch { [int]$_.Exception.Response.StatusCode }
  }
}
$codes = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job

$after = Balance; $txAfter = Txns
"key: $(if ($NoKey) {'<none>'} else {$key})  header: $HeaderName"
"status codes: " + (($codes | Group-Object | ForEach-Object { "$($_.Name) x$($_.Count)" }) -join ', ')
"balance: $before -> $after  (booked deposits of 1: $([int]($after - $before)))"
"transactions: $txBefore -> $txAfter"
if (-not $NoKey -and ($after - $before) -gt 1) { "RESULT: DUPLICATE POSTINGS - idempotency not effective under concurrency" }
elseif (-not $NoKey) { "RESULT: idempotent (1 posting)" }
