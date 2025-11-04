# scripts\smoke.ps1
$ok= $true
$ok = $ok -and ((docker inspect -f "{{.State.Health.Status}}" attrib-postgres) -eq "healthy")
$ok = $ok -and (Invoke-WebRequest http://localhost:8081 -UseBasicParsing).StatusCode -eq 200  # Adminer
$ok = $ok -and (Invoke-WebRequest http://localhost:3000 -UseBasicParsing).StatusCode -eq 200  # Metabase
if(-not $ok){ Write-Host "SMOKE CHECK FAILED"; exit 1 } else { Write-Host "SMOKE CHECK PASSED"; exit 0 }
