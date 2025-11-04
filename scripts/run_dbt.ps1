# --- bootstrap paths ---
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

# --- rotate logs before anything else ---
$rotate = Join-Path $PSScriptRoot 'rotate_logs.ps1'
if (Test-Path $rotate) {
  & $rotate
} else {
  Write-Warning "rotate_logs.ps1 not found at $rotate"
}


$repo = "C:\Users\danie\attribution-os\analytics\attribution_dbt"
$venv = "C:\Users\danie\attribution-os\.venv\Scripts\Activate.ps1"

cd $repo
. $venv
$env:DBT_PROFILES_DIR = "$repo\profiles"

dbt seed --full-refresh
dbt run
dbt test
dbt snapshot --select orders_snapshot



# ... (tu pipeline: dbt seed/run/test/snapshot + logs)

Write-Host "Running smoke checks..."
$smoke = powershell -NoProfile -ExecutionPolicy Bypass -File "$PSScriptRoot\smoke.ps1"
$code = $LASTEXITCODE
Write-Host $smoke
if ($code -ne 0) {
  Write-Host "Run failed due to smoke checks."
  exit $code
} else {
  Write-Host "Run finished OK."
  exit 0
}
