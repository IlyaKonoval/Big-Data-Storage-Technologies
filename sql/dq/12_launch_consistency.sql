-- check: launch_consistency
-- severity: warning
-- description: All objects of one launch share launch date and site in the last SATCAT snapshot
SELECT launch_designator, groupUniqArray((launch_date, launch_site)) AS variants
FROM stage.satcat
WHERE launch_designator IS NOT NULL
GROUP BY launch_designator
HAVING length(variants) > 1
