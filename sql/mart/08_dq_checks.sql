-- mart: dq_checks
-- insight: the latest result of every data quality check (what failed, how many rows,
--          examples) for the health tab of the dashboard.
-- order_by: (severity, check_name)
SELECT check_name, severity, description, violations, passed, sample, checked_at
FROM meta.dq_results
WHERE run_id = (SELECT argMax(run_id, checked_at) FROM meta.dq_results)
