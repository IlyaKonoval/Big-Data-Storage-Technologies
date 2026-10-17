-- Multi-active delta load: if anything in the set of TCAs of a pair changed (a TCA was
-- refined, added or removed), the whole new set is written as one version.
INSERT INTO rv.sat_conjunction
    (hk_conjunction, load_dts, tca, hashdiff, set_hashdiff, record_source, dse_1, dse_2,
     tca_range, tca_relative_speed, max_prob, dilution)
SELECT s.hk_conjunction, s.load_dts, s.tca, s.hashdiff, s.set_hashdiff, s.record_source,
       s.dse_1, s.dse_2, s.tca_range, s.tca_relative_speed, s.max_prob, s.dilution
FROM stage.socrates AS s
LEFT JOIN
(
    SELECT hk_conjunction, argMax(set_hashdiff, load_dts) AS set_hashdiff
    FROM rv.sat_conjunction
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.socrates)
    GROUP BY hk_conjunction
) AS prev ON prev.hk_conjunction = s.hk_conjunction
WHERE prev.set_hashdiff != s.set_hashdiff;
