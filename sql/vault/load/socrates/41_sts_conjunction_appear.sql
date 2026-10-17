-- Status tracking: pairs present in this forecast whose last status is not 'I' (new pairs
-- or pairs that come back) get status 'I'.
INSERT INTO rv.sts_conjunction (hk_conjunction, load_dts, status, record_source)
SELECT s.hk_conjunction, any(s.load_dts), 'I', any(s.record_source)
FROM stage.socrates AS s
LEFT JOIN
(
    SELECT hk_conjunction, argMax(status, load_dts) AS status
    FROM rv.sts_conjunction
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.socrates)
    GROUP BY hk_conjunction
) AS prev ON prev.hk_conjunction = s.hk_conjunction
WHERE prev.status != 'I'
GROUP BY s.hk_conjunction;
