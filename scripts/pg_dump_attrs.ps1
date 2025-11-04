# scripts/pg_dump_attrs.ps1
# Fail fast y paths
$ErrorActionPreference = 'Stop'
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

# ===== Config =====
$Container = "attrib-postgres"
$DbName    = "attrib_analytics"
$DbUser    = "attrib"
$DbPass    = "attrib"
$BackupDir = "C:\Users\danie\attribution-os\backups"

# Asegura carpeta de backups
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

# Timestamp y rutas
$stamp      = Get-Date -Format "yyyyMMdd"
$remoteFile = "/tmp/attrib_analytics_${stamp}.dump"
$localFile  = Join-Path $BackupDir ("attrib_analytics_{0}.dump" -f $stamp)

try {
  # 0) Sanity check: contenedor corriendo
  docker ps --format "{{.Names}}" | Select-String -SimpleMatch "^$Container$" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "El contenedor '$Container' no está en ejecución." }

  # 1) pg_dump dentro del contenedor (formato custom -Fc)
  $cmd = "PGPASSWORD=$DbPass pg_dump -U $DbUser -d $DbName -Fc -f $remoteFile"
  docker exec $Container bash -lc "$cmd"
  if ($LASTEXITCODE -ne 0) { throw "pg_dump falló con exitcode $LASTEXITCODE" }

  # 2) Copia al host (sin comodines)
  docker cp "$Container:$remoteFile" "$localFile"
  if ($LASTEXITCODE -ne 0) { throw "docker cp falló con exitcode $LASTEXITCODE" }

  # 3) Verifica tamaño del archivo
  $fi = Get-Item $localFile -ErrorAction Stop
  if (-not $fi -or $fi.Length -le 0) { throw "Backup copiado pero vacío." }

  # 4) (Opcional) Limpia backups >14 días
  Get-ChildItem $BackupDir -Filter "attrib_analytics_*.dump" -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } |
    Remove-Item -Force -ErrorAction SilentlyContinue

  Write-Host "Backup OK -> $localFile (tamaño: $($fi.Length) bytes)"
  exit 0
}
catch {
  Write-Error $_
  exit 1
}

