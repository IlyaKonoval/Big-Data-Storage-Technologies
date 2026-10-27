-- check: pit_complete
-- severity: critical
-- description: В сегодняшнем снимке PIT есть строка для каждого объекта хаба
SELECT today() AS snapshot_date, hub_objects, pit_rows
FROM
(
    SELECT (SELECT uniqExact(hk_object) FROM rv.hub_object) AS hub_objects,
           (SELECT count() FROM bv.pit_object FINAL WHERE snapshot_date = today()) AS pit_rows
)
WHERE hub_objects != pit_rows
