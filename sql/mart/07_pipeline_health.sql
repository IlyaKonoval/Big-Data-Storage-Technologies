-- mart: pipeline_health
-- insight: is the platform healthy: per source the last successful download, hours since,
--          snapshots kept, raw volume, rows of the last snapshot, snapshots still waiting
--          for the vault; plus the summary of the last data quality run.
-- order_by: source
WITH (SELECT argMax(run_id, checked_at) FROM meta.dq_results) AS last_dq_run
SELECT f.source AS source,
       max(f.fetched_at) AS last_fetched_at,
       round(dateDiff('second', max(f.fetched_at), now64(6)) / 3600, 1) AS hours_since_fetch,
       count() AS snapshots,
       sum(f.bytes) AS raw_bytes,
       argMax(l.rows_loaded, f.fetched_at) AS last_snapshot_rows,
       countIf(f.load_id NOT IN (SELECT load_id FROM meta.vault_log FINAL)) AS snapshots_not_in_vault,
       (SELECT max(checked_at) FROM meta.dq_results) AS dq_checked_at,
       (SELECT countIf(passed) FROM meta.dq_results WHERE run_id = last_dq_run) AS dq_passed,
       (SELECT countIf(NOT passed AND severity = 'critical') FROM meta.dq_results
        WHERE run_id = last_dq_run) AS dq_failed_critical,
       (SELECT countIf(NOT passed AND severity = 'warning') FROM meta.dq_results
        WHERE run_id = last_dq_run) AS dq_warnings
FROM meta.raw_files AS f FINAL
INNER JOIN (SELECT * FROM meta.load_log FINAL
            WHERE target_db = 'clickhouse' AND status = 'success') AS l
        ON l.load_id = f.load_id
GROUP BY f.source
