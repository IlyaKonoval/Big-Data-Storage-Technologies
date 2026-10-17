-- check: conjunction_ranges
-- severity: critical
-- description: Conjunction values are valid: range >= 0, relative speed >= 0, 0 <= probability <= 1
SELECT hk_conjunction, load_dts, tca, tca_range, tca_relative_speed, max_prob
FROM rv.sat_conjunction
WHERE NOT ifNull(tca_range >= 0 AND tca_relative_speed >= 0 AND max_prob BETWEEN 0 AND 1, false)
