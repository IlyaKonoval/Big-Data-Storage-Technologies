-- Point-in-time rows from the last built day up to today (the last day is rebuilt, since
-- loads keep arriving during the day; ReplacingMergeTree keeps the newest built_at).
-- ASOF JOIN picks, per object, the latest satellite version loaded before the next midnight.
INSERT INTO bv.pit_object
    (snapshot_date, hk_object, desc_ldts, status_ldts, gp_ldts, supgp_ldts, built_at)
WITH
    (SELECT toDate(min(load_dts)) FROM rv.hub_object) AS first_day,
    (SELECT if(count() = 0, first_day, max(snapshot_date)) FROM bv.pit_object) AS start_day
SELECT p.snapshot_date, p.hk_object, d.load_dts, st.load_dts, g.load_dts, sg.load_dts, now64(6)
FROM
(
    SELECT days.snapshot_date, toDateTime64(days.snapshot_date + 1, 6) AS next_day, h.hk_object
    FROM
    (
        SELECT arrayJoin(arrayMap(i -> start_day + i,
                                  range(toUInt32(dateDiff('day', start_day, today()) + 1))))
               AS snapshot_date
    ) AS days
    CROSS JOIN (SELECT hk_object, min(load_dts) AS first_dts
                FROM rv.hub_object GROUP BY hk_object) AS h
    WHERE h.first_dts < next_day
) AS p
ASOF LEFT JOIN (SELECT hk_object, load_dts FROM rv.sat_object_satcat_desc) AS d
    ON p.hk_object = d.hk_object AND p.next_day > d.load_dts
ASOF LEFT JOIN (SELECT hk_object, load_dts FROM rv.sat_object_satcat_status) AS st
    ON p.hk_object = st.hk_object AND p.next_day > st.load_dts
ASOF LEFT JOIN (SELECT hk_object, load_dts FROM rv.sat_object_orbit_gp) AS g
    ON p.hk_object = g.hk_object AND p.next_day > g.load_dts
ASOF LEFT JOIN (SELECT hk_object, load_dts FROM rv.sat_object_orbit_supgp) AS sg
    ON p.hk_object = sg.hk_object AND p.next_day > sg.load_dts
SETTINGS join_use_nulls = 1;
