# Creates N merchants for load tests: one Fineract client + savings account each, mapped in ledger-service.
#   powershell -File scripts/seed-load-merchants.ps1 -Count 10
# Merchant ids are load-0 .. load-(N-1), matching perf/order-flow.js with MERCHANTS=N. Safe to re-run.
param([int]$Count = 10)
$ErrorActionPreference = 'Stop'
$base = 'http://localhost:8443/fineract-provider/api/v1'
$h = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('mifos:password')); 'Fineract-Platform-TenantId' = 'default' }
$fmt = @{ dateFormat = 'dd MMMM yyyy'; locale = 'en' }
$date = '01 January 2026'
function Post($path, $body) { Invoke-RestMethod -Method Post -Uri "$base/$path" -Headers $h -ContentType 'application/json' -Body ($body | ConvertTo-Json -Depth 5) }

$product = (Invoke-RestMethod -Uri "$base/savingsproducts" -Headers $h | Where-Object { $_.name -eq 'Wallet' } | Select-Object -First 1).id
if (-not $product) { throw "Savings product 'Wallet' not found. Run scripts/first-deposit.ps1 once first." }

0..($Count - 1) | ForEach-Object {
  $merchant = "load-$_"
  $exists = docker exec finract_jenny-postgres-1 psql -U postgres -d ledger -t -A -c "select count(*) from merchant_accounts where merchant_id = '$merchant'"
  if ([int]$exists -gt 0) { "${merchant}: already mapped"; return }
  $client = Post 'clients' (@{ officeId = 1; legalFormId = 1; firstname = 'Load'; lastname = $merchant; active = $true; activationDate = $date; submittedOnDate = $date } + $fmt)
  $acct = (Post 'savingsaccounts' (@{ clientId = $client.clientId; productId = $product; submittedOnDate = $date } + $fmt)).savingsId
  Post "savingsaccounts/${acct}?command=approve" (@{ approvedOnDate = $date } + $fmt) | Out-Null
  Post "savingsaccounts/${acct}?command=activate" (@{ activatedOnDate = $date } + $fmt) | Out-Null
  docker exec finract_jenny-postgres-1 psql -U postgres -d ledger -c "insert into merchant_accounts (merchant_id, fineract_client_id, fineract_savings_id) values ('$merchant', $($client.clientId), $acct) on conflict do nothing" | Out-Null
  "$merchant -> client $($client.clientId), savings account $acct"
}
