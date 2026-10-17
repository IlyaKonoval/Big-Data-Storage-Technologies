-- Delta load: a new version only for objects whose element set changed since the latest
-- version (usually a new epoch). Repeated GP files with few updates add few rows.
INSERT INTO rv.sat_object_orbit_gp
    (hk_object, load_dts, hashdiff, record_source, object_name, object_id, epoch, mean_motion,
     eccentricity, inclination, ra_of_asc_node, arg_of_pericenter, mean_anomaly,
     ephemeris_type, classification_type, element_set_no, rev_at_epoch, bstar,
     mean_motion_dot, mean_motion_ddot)
SELECT s.hk_object, s.load_dts, s.hashdiff, s.record_source, s.object_name, s.object_id,
       s.epoch, s.mean_motion, s.eccentricity, s.inclination, s.ra_of_asc_node,
       s.arg_of_pericenter, s.mean_anomaly, s.ephemeris_type, s.classification_type,
       s.element_set_no, s.rev_at_epoch, s.bstar, s.mean_motion_dot, s.mean_motion_ddot
FROM stage.gp AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hashdiff
    FROM rv.sat_object_orbit_gp
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.gp)
    GROUP BY hk_object
) AS prev ON prev.hk_object = s.hk_object
WHERE prev.hashdiff != s.hashdiff;
