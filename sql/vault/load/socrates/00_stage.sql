-- Staging of one SOCRATES forecast.
-- Cleaning: pairs are unordered in the source (id1 > id2 in ~23% of rows), so the pair is
-- normalized to (smaller NORAD, larger NORAD) and dse_1/dse_2 are swapped with it;
-- self-pairs and rows without TCA are dropped; duplicates of (pair, tca) are removed.
-- set_hashdiff covers all TCAs of a pair: the multi-active satellite is versioned per set.
CREATE TABLE stage.socrates
ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    hk_object_1, hk_object_2, hk_conjunction, norad_cat_id_1, norad_cat_id_2, tca,
    dse_1, dse_2, tca_range, tca_relative_speed, max_prob, dilution, hashdiff,
    dv_hash(arrayStringConcat(arraySort(groupArray(hashdiff) OVER (
        PARTITION BY hk_conjunction
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)), ',')) AS set_hashdiff,
    load_dts, record_source
FROM
(
    SELECT
        dv_hash(toString(id_1)) AS hk_object_1,
        dv_hash(toString(id_2)) AS hk_object_2,
        dv_hash(concat(toString(id_1), '||', toString(id_2))) AS hk_conjunction,
        id_1 AS norad_cat_id_1,
        id_2 AS norad_cat_id_2,
        tca_ AS tca,
        dse_a AS dse_1,
        dse_b AS dse_2,
        tca_range, tca_relative_speed, max_prob, dilution,
        dv_hash(concatWithSeparator('||',
            toString(tca_), ifNull(toString(dse_a), ''), ifNull(toString(dse_b), ''),
            ifNull(toString(tca_range), ''), ifNull(toString(tca_relative_speed), ''),
            ifNull(toString(max_prob), ''), ifNull(toString(dilution), ''))) AS hashdiff,
        loaded_at AS load_dts,
        source_file AS record_source
    FROM
    (
        SELECT
            least(norad_cat_id_1, norad_cat_id_2) AS id_1,
            greatest(norad_cat_id_1, norad_cat_id_2) AS id_2,
            norad_cat_id_1 < norad_cat_id_2 AS in_order,
            if(in_order, dse_1, dse_2) AS dse_a,
            if(in_order, dse_2, dse_1) AS dse_b,
            assumeNotNull(tca) AS tca_,
            tca_range, tca_relative_speed, max_prob, dilution, loaded_at, source_file
        FROM ods.socrates
        WHERE load_id = {load_id:String}
          AND norad_cat_id_1 != norad_cat_id_2
          AND tca IS NOT NULL
        ORDER BY id_1, id_2, tca_, tca_range
        LIMIT 1 BY id_1, id_2, tca_
    )
);
