-- check: orphan_links
-- severity: critical
-- description: Каждый линк ссылается на существующие ключи хабов (в ClickHouse нет внешних ключей)
SELECT 'link_object_launch' AS link, hk_object_launch AS hk
FROM rv.link_object_launch
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
   OR hk_launch NOT IN (SELECT hk_launch FROM rv.hub_launch)
UNION ALL
SELECT 'link_conjunction', hk_conjunction
FROM rv.link_conjunction
WHERE hk_object_1 NOT IN (SELECT hk_object FROM rv.hub_object)
   OR hk_object_2 NOT IN (SELECT hk_object FROM rv.hub_object)
