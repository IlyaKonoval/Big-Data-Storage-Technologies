-- Delta load, same pattern as sat_object_satcat_desc.
INSERT INTO rv.sat_object_satcat_status
    (hk_object, load_dts, hashdiff, record_source, ops_status_code, decay_date,
     data_status_code, orbit_type, rcs, period, inclination, apogee, perigee)
SELECT s.hk_object, s.load_dts, s.hd_status, s.record_source, s.ops_status_code, s.decay_date,
       s.data_status_code, s.orbit_type, s.rcs, s.period, s.inclination, s.apogee, s.perigee
FROM stage.satcat AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hashdiff
    FROM rv.sat_object_satcat_status
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.satcat)
    GROUP BY hk_object
) AS prev ON prev.hk_object = s.hk_object
WHERE prev.hashdiff != s.hd_status;
