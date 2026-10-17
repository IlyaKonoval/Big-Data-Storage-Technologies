-- check: gp_freshness
-- severity: warning
-- description: The newest GP snapshot in the vault was downloaded less than 8 hours ago
SELECT max(f.fetched_at) AS last_gp_download
FROM meta.vault_log AS v FINAL
INNER JOIN meta.raw_files AS f FINAL ON f.load_id = v.load_id
WHERE v.source = 'gp'
HAVING last_gp_download < now64(6) - INTERVAL 8 HOUR
