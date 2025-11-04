param([string]$DumpPath)
$Container="attrib-postgres"; $DbUser="attrib"; $DbPass="attrib"
$TmpDb="attrib_analytics_tmp"

docker cp "$DumpPath" $Container:/tmp/restore.dump
docker exec $Container bash -lc "PGPASSWORD=$DbPass dropdb -U $DbUser --if-exists $TmpDb && PGPASSWORD=$DbPass createdb -U $DbUser $TmpDb && PGPASSWORD=$DbPass pg_restore -U $DbUser -d $TmpDb -c /tmp/restore.dump"
