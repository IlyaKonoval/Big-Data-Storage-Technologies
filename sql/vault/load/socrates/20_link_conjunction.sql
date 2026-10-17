INSERT INTO rv.link_conjunction (hk_conjunction, hk_object_1, hk_object_2, load_dts, record_source)
SELECT hk_conjunction, any(hk_object_1), any(hk_object_2), any(load_dts), any(record_source)
FROM stage.socrates
WHERE hk_conjunction NOT IN (SELECT hk_conjunction FROM rv.link_conjunction)
GROUP BY hk_conjunction;
