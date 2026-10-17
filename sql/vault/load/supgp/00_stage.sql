-- Staging of one SupGP snapshot (operator ephemerides): GP columns + rms, data_source.
CREATE TABLE stage.supgp
ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    dv_hash(toString(norad_cat_id)) AS hk_object,
    norad_cat_id,
    object_name, object_id, epoch, mean_motion, eccentricity, inclination, ra_of_asc_node,
    arg_of_pericenter, mean_anomaly, ephemeris_type, classification_type, element_set_no,
    rev_at_epoch, bstar, mean_motion_dot, mean_motion_ddot, rms, data_source,
    dv_hash(concatWithSeparator('||',
        ifNull(object_name, ''), ifNull(object_id, ''), ifNull(toString(epoch), ''),
        ifNull(toString(mean_motion), ''), ifNull(toString(eccentricity), ''),
        ifNull(toString(inclination), ''), ifNull(toString(ra_of_asc_node), ''),
        ifNull(toString(arg_of_pericenter), ''), ifNull(toString(mean_anomaly), ''),
        ifNull(toString(ephemeris_type), ''), ifNull(classification_type, ''),
        ifNull(toString(element_set_no), ''), ifNull(toString(rev_at_epoch), ''),
        ifNull(toString(bstar), ''), ifNull(toString(mean_motion_dot), ''),
        ifNull(toString(mean_motion_ddot), ''), ifNull(toString(rms), ''),
        ifNull(data_source, ''))) AS hashdiff,
    loaded_at AS load_dts,
    source_file AS record_source
FROM ods.supgp
WHERE load_id = {load_id:String}
ORDER BY norad_cat_id, epoch DESC
LIMIT 1 BY norad_cat_id;
