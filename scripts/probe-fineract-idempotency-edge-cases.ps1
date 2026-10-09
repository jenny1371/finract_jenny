# Edge cases of Fineract's Idempotency-Key handling, observed on a real instance. Evidence for FINERACT-2485.
#   powershell -File scripts/probe-fineract-idempotency-edge-cases.ps1 -AccountA 2 -AccountB 3
# Each scenario prints: HTTP status of every call, and how the account balances actually moved.
param([int]$AccountA = 2, [int]$AccountB = 3, [int]$PaymentTypeId = 4)
$base = 'http://localhost:8443/fineract-provider/api/v1'
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('mifos:password'))
$today = (Get-Date).AddDays(-1).ToString('dd MMMM yyyy', [Globalization.CultureInfo]'en-US')

function Balance($acct) { [decimal](Invoke-RestMethod -Uri "$base/savingsaccounts/$acct" -Headers @{ Authorization = $auth; 'Fineract-Platform-TenantId' = 'default' }).summary.accountBalance }
function Deposit($acct, $amount, $key, $headerName = 'Idempotency-Key') {
  $body = @{ transactionDate = $today; transactionAmount = $amount; paymentTypeId = $PaymentTypeId; dateFormat = 'dd MMMM yyyy'; locale = 'en' } | ConvertTo-Json -Compress
  $h = @{ Authorization = $auth; 'Fineract-Platform-TenantId' = 'default' }
  if ($key) { $h[$headerName] = $key }
  try {
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$base/savingsaccounts/$acct/transactions?command=deposit" -Headers $h -ContentType 'application/json' -Body $body
    return [pscustomobject]@{ Status = [int]$r.StatusCode; Body = $r.Content }
  } catch {
    $resp = $_.Exception.Response
    $text = if ($resp) { (New-Object IO.StreamReader($resp.GetResponseStream())).ReadToEnd() } else { $_.Exception.Message }
    return [pscustomobject]@{ Status = if ($resp) { [int]$resp.StatusCode } else { -1 }; Body = $text }
  }
}
function Short($s) { if ($s.Length -gt 110) { $s.Substring(0, 110) + '...' } else { $s } }
function Scenario($title, [scriptblock]$run) {
  $a0 = Balance $AccountA; $b0 = Balance $AccountB
  "`n=== $title"
  & $run
  "    balance A: {0} -> {1} (delta {2})   balance B: {3} -> {4} (delta {5})" -f $a0, (Balance $AccountA), ((Balance $AccountA) - $a0), $b0, (Balance $AccountB), ((Balance $AccountB) - $b0)
}
function Show($label, $r) { "    {0,-34} HTTP {1}  {2}" -f $label, $r.Status, (Short $r.Body) }

Scenario "1. control: same key, same body, twice (expect 1 posting, replay)" {
  $k = [guid]::NewGuid().ToString()
  Show 'first (amount 1)' (Deposit $AccountA 1 $k)
  Show 'second (amount 1)' (Deposit $AccountA 1 $k)
}
Scenario "2. same key, DIFFERENT body (amount 1, then amount 2)" {
  $k = [guid]::NewGuid().ToString()
  Show 'first (amount 1)' (Deposit $AccountA 1 $k)
  Show 'second (amount 2)' (Deposit $AccountA 2 $k)
}
Scenario "3. failed request, then CORRECTED retry with the same key (amount -5, then amount 1)" {
  $k = [guid]::NewGuid().ToString()
  Show 'first (invalid amount -5)' (Deposit $AccountA -5 $k)
  Show 'retry (valid amount 1)' (Deposit $AccountA 1 $k)
}
Scenario "4. same key on a DIFFERENT account (A then B)" {
  $k = [guid]::NewGuid().ToString()
  Show 'account A (amount 1)' (Deposit $AccountA 1 $k)
  Show 'account B (amount 1)' (Deposit $AccountB 1 $k)
}
Scenario "5. header name in lower case (idempotency-key), twice" {
  $k = [guid]::NewGuid().ToString()
  Show 'first' (Deposit $AccountA 1 $k 'idempotency-key')
  Show 'second' (Deposit $AccountA 1 $k 'idempotency-key')
}
Scenario "6. very long key (1000 characters), twice" {
  $k = 'k' * 1000
  Show 'first' (Deposit $AccountA 1 $k)
  Show 'second' (Deposit $AccountA 1 $k)
}
Scenario "7. empty key header value (treated as no key?), twice" {
  Show 'first' (Deposit $AccountA 1 '')
  Show 'second' (Deposit $AccountA 1 '')
}
