# Starts one service locally with the secrets from .env loaded as environment variables.
#   powershell -File scripts/run-service.ps1 -Service payment-service
# Prerequisite: docker compose -f docker-compose.yml -f docker-compose.infra.yml up -d postgres kafka redis [fineract fineract-db]
param([Parameter(Mandatory)][ValidateSet('order-service','payment-service','ledger-service','reconciliation-service')][string]$Service)
$root = Split-Path -Parent $PSScriptRoot
Get-Content "$root\.env" | Where-Object { $_ -match '^\s*[^#].*=' } | ForEach-Object {
  $k, $v = $_ -split '=', 2
  [Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), 'Process')
}
$mvn = if (Get-Command mvn -ErrorAction SilentlyContinue) { 'mvn' } else { "$env:USERPROFILE\tools\apache-maven-3.9.9\bin\mvn.cmd" }
Set-Location $root
& $mvn -q -DskipTests install -pl libs/platform-common
& $mvn -pl "services/$Service" spring-boot:run
