-- check: launch_consistency
-- severity: warning
-- description: У всех объектов одного запуска совпадают дата и площадка запуска в последнем снимке SATCAT
SELECT launch_designator, groupUniqArray((launch_date, launch_site)) AS variants
FROM stage.satcat
WHERE launch_designator IS NOT NULL
GROUP BY launch_designator
HAVING length(variants) > 1
