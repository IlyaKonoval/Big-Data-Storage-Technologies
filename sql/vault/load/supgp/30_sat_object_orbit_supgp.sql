-- Delta load, same pattern as sat_object_orbit_gp: a separate satellite per source system,
-- so GP and SupGP versions of the same object never overwrite each other.
INSERT INTO rv.sat_object_orbit_supgp
    (hk_object, load_dts, hashdiff, record_source, object_name, object_id, epoch, mean_motion,
     eccentricity, inclination, ra_of_asc_node, arg_of_pericenter, mean_anomaly,
     ephemeris_type, classification_type, element_set_no, rev_at_epoch, bstar,
     mean_motion_dot, mean_motion_ddot, rms, data_source)
SELECT s.hk_object, s.load_dts, s.hashdiff, s.record_source, s.object_name, s.object_id,
       s.epoch, s.mean_motion, s.eccentricity, s.inclination, s.ra_of_asc_node,
       s.arg_of_pericenter, s.mean_anomaly, s.ephemeris_type, s.classification_type,
       s.element_set_no, s.rev_at_epoch, s.bstar, s.mean_motion_dot, s.mean_motion_ddot,
       s.rms, s.data_source
FROM stage.supgp AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hashdiff
    FROM rv.sat_object_orbit_supgp
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.supgp)
    GROUP BY hk_object
) AS prev ON prev.hk_object = s.hk_object
WHERE prev.hashdiff != s.hashdiff;
