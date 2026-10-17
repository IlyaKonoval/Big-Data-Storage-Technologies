-- Status tracking: pairs that were in the previous forecast but are absent from this one
-- get status 'D' (the forecast no longer predicts the approach).
INSERT INTO rv.sts_conjunction (hk_conjunction, load_dts, status, record_source)
SELECT p.hk_conjunction, l.load_dts, 'D', l.record_source
FROM
(
    SELECT hk_conjunction, argMax(status, load_dts) AS status, max(load_dts) AS last_dts
    FROM rv.sts_conjunction
    GROUP BY hk_conjunction
) AS p
CROSS JOIN
(
    SELECT any(load_dts) AS load_dts, any(record_source) AS record_source
    FROM stage.socrates
) AS l
WHERE p.status = 'I'
  AND p.last_dts < l.load_dts
  AND p.hk_conjunction NOT IN (SELECT hk_conjunction FROM stage.socrates);
