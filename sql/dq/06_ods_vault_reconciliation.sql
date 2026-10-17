-- check: ods_vault_reconciliation
-- severity: critical
-- description: Volumes before/after: every business key of the last vaulted ODS snapshot of each source is in the vault
WITH last AS
(
    SELECT source, argMax(load_id, finished_at) AS load_id
    FROM meta.vault_log FINAL
    GROUP BY source
)
SELECT source, snapshot, ods_keys, vault_keys
FROM
(
    SELECT 'satcat' AS source, any(load_id) AS snapshot,
           uniqExact(norad_cat_id) AS ods_keys,
           uniqExactIf(norad_cat_id, dv_hash(toString(norad_cat_id))
                       IN (SELECT hk_object FROM rv.hub_object)) AS vault_keys
    FROM ods.satcat
    WHERE load_id IN (SELECT load_id FROM last WHERE source = 'satcat')
    UNION ALL
    SELECT 'gp', any(load_id), uniqExact(norad_cat_id),
           uniqExactIf(norad_cat_id, dv_hash(toString(norad_cat_id))
                       IN (SELECT hk_object FROM rv.hub_object))
    FROM ods.gp
    WHERE load_id IN (SELECT load_id FROM last WHERE source = 'gp')
    UNION ALL
    SELECT 'supgp', any(load_id), uniqExact(norad_cat_id),
           uniqExactIf(norad_cat_id, dv_hash(toString(norad_cat_id))
                       IN (SELECT hk_object FROM rv.hub_object))
    FROM ods.supgp
    WHERE load_id IN (SELECT load_id FROM last WHERE source = 'supgp')
    UNION ALL
    -- SOCRATES: distinct unordered pairs vs pairs in link_conjunction
    SELECT 'socrates', any(load_id), uniqExact(pair_key),
           uniqExactIf(pair_key, pair_key IN (SELECT hk_conjunction FROM rv.link_conjunction))
    FROM
    (
        SELECT load_id,
               dv_hash(concat(toString(least(norad_cat_id_1, norad_cat_id_2)), '||',
                              toString(greatest(norad_cat_id_1, norad_cat_id_2)))) AS pair_key
        FROM ods.socrates
        WHERE load_id IN (SELECT load_id FROM last WHERE source = 'socrates')
          AND norad_cat_id_1 != norad_cat_id_2
    )
)
WHERE ods_keys != vault_keys
