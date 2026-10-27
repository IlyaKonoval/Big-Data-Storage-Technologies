-- check: duplicate_keys
-- severity: warning
-- description: Строки с одинаковым ключом, ещё не схлопнутые ReplacingMergeTree (параллельные загрузки); читатели используют FINAL
SELECT 'hub_object' AS tbl, hk_object AS k, count() AS n
FROM rv.hub_object GROUP BY hk_object HAVING n > 1
UNION ALL
SELECT 'hub_launch', hk_launch, count() AS n
FROM rv.hub_launch GROUP BY hk_launch HAVING n > 1
UNION ALL
SELECT 'link_object_launch', hk_object_launch, count() AS n
FROM rv.link_object_launch GROUP BY hk_object_launch HAVING n > 1
UNION ALL
SELECT 'link_conjunction', hk_conjunction, count() AS n
FROM rv.link_conjunction GROUP BY hk_conjunction HAVING n > 1
