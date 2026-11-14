# Uploads the provider API keys from your local .env to the GitHub Actions secrets of this repo.
# Values are read locally and sent straight to GitHub; nothing is printed except key NAMES.
#
# One-time setup:  winget install --id GitHub.cli    then    gh auth login
# Run (from the project folder):
#   powershell -ExecutionPolicy Bypass -File tools\set_github_secrets.ps1
#   powershell -ExecutionPolicy Bypass -File tools\set_github_secrets.ps1 -Repo owner/name -DryRun
param(
    [string]$Repo = "himesh-mehta/Weather-Forecast-",
    [string]$EnvFile = (Join-Path $PSScriptRoot "..\.env"),
    [switch]$DryRun
)

# Only the keys the workflow uses (see .github/workflows/weather_ingest.yml) - never DATABASE_URL etc.
$Names = @(
    "WEATHERAPI_KEY", "OPENWEATHER_KEY", "VISUAL_CROSSING_KEY", "TOMORROW_IO_KEY", "WEATHERBIT_KEY",
    "METEOSOURCE_KEY", "ACCUWEATHER_KEY", "METEOBLUE_KEY", "WINDY_KEY", "STORMGLASS_KEY", "AMBEE_KEY",
    "GOOGLE_WEATHER_KEY", "APPLE_WEATHERKIT_JWT", "METOFFICE_DATAHUB_KEY", "XWEATHER_CLIENT_ID",
    "XWEATHER_CLIENT_SECRET", "WEATHER_UNION_KEY", "FORECA_KEY"
)

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI not found. Install it with:  winget install --id GitHub.cli   then run:  gh auth login"
}
if (-not (Test-Path $EnvFile)) { throw "No .env found at $EnvFile" }

# Parse .env; like python-dotenv, a later definition of the same name wins.
$Values = @{}
foreach ($line in Get-Content $EnvFile) {
    if ($line -match '^\s*([A-Za-z0-9_]+)\s*=(.*)$') { $Values[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'") }
}

$set = 0
foreach ($n in $Names) {
    $v = $Values[$n]
    if ([string]::IsNullOrWhiteSpace($v)) { Write-Output ("skipped  {0}  (empty in .env)" -f $n); continue }
    if ($DryRun) { Write-Output ("would set {0}" -f $n); continue }
    $v | gh secret set $n --repo $Repo          # value goes via stdin, never on the command line
    if ($LASTEXITCODE -ne 0) { throw "gh secret set failed for $n" }
    Write-Output ("set      {0}" -f $n); $set++
}
Write-Output ""
Write-Output "$set secrets set on $Repo. Start a test run with:"
Write-Output "  gh workflow run weather_ingest.yml --repo $Repo -f tier=full"
