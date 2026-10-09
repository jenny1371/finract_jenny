# Step 1: client -> savings product -> savings account -> approve -> activate -> deposit.
# Usage: powershell -File scripts/first-deposit.ps1   (safe to re-run; reuses product and payment type)
$ErrorActionPreference = 'Stop'
$base = 'http://localhost:8443/fineract-provider/api/v1'
$h = @{
  Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('mifos:password'))
  'Fineract-Platform-TenantId' = 'default'
}
$fmt = @{ dateFormat = 'dd MMMM yyyy'; locale = 'en' }
$date = '01 January 2026'

function Post($path, $body) {
  Invoke-RestMethod -Method Post -Uri "$base/$path" -Headers $h -ContentType 'application/json' -Body ($body | ConvertTo-Json -Depth 5)
}
function Get-Existing($path, $name) {
  Invoke-RestMethod -Uri "$base/$path" -Headers $h | Where-Object { $_.name -eq $name } | Select-Object -First 1
}

$client = Post 'clients' (@{ officeId = 1; legalFormId = 1; firstname = 'Test'; lastname = 'User'; active = $true;
  activationDate = $date; submittedOnDate = $date } + $fmt)
"client id: $($client.clientId)"

$product = Get-Existing 'savingsproducts' 'Wallet'
if ($product) { $productId = $product.id } else {
  $productId = (Post 'savingsproducts' @{ name = 'Wallet'; shortName = 'WLT'; currencyCode = 'USD'; digitsAfterDecimal = 2;
    inMultiplesOf = 0; nominalAnnualInterestRate = 0; interestCompoundingPeriodType = 1; interestPostingPeriodType = 4;
    interestCalculationType = 1; interestCalculationDaysInYearType = 365; accountingRule = 1; locale = 'en' }).resourceId
}
"product id: $productId"

# Fineract requires paymentTypeId on every deposit.
$pt = Get-Existing 'paymenttypes' 'Stripe'
if ($pt) { $ptId = $pt.id } else {
  $ptId = (Post 'paymenttypes' @{ name = 'Stripe'; description = 'Stripe card payment'; isCashPayment = $false; position = 1 }).resourceId
}
"payment type id: $ptId"

$id = (Post 'savingsaccounts' (@{ clientId = $client.clientId; productId = $productId; submittedOnDate = $date } + $fmt)).savingsId
"savings account id: $id"

Post "savingsaccounts/${id}?command=approve" (@{ approvedOnDate = $date } + $fmt) | Out-Null
Post "savingsaccounts/${id}?command=activate" (@{ activatedOnDate = $date } + $fmt) | Out-Null

$dep = Post "savingsaccounts/$id/transactions?command=deposit" (@{ transactionDate = $date; transactionAmount = 100; paymentTypeId = $ptId } + $fmt)
"deposit transaction id: $($dep.resourceId)"

$final = Invoke-RestMethod -Uri "$base/savingsaccounts/$id" -Headers $h
"balance: $($final.summary.accountBalance) $($final.currency.code)"
