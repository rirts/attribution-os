$dir="$PSScriptRoot\..\logs"; New-Item -ItemType Directory -Force -Path $dir | Out-Null
Get-ChildItem $dir -File | Where-Object {$_.LastWriteTime -lt (Get-Date).AddDays(-14)} | Remove-Item -Force -ErrorAction SilentlyContinue
