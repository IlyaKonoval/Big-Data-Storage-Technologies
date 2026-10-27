-- check: hub_key_consistency
-- severity: critical
-- description: У каждого NORAD ID и запуска ровно один хеш-ключ, равный md5 бизнес-ключа
SELECT 'hub_object' AS hub, toString(norad_cat_id) AS business_key, groupUniqArray(hk_object) AS keys
FROM rv.hub_object
GROUP BY norad_cat_id
HAVING length(keys) > 1 OR keys[1] != dv_hash(toString(norad_cat_id))
UNION ALL
SELECT 'hub_launch', launch_designator, groupUniqArray(hk_launch) AS keys
FROM rv.hub_launch
GROUP BY launch_designator
HAVING length(keys) > 1 OR keys[1] != dv_hash(launch_designator)
