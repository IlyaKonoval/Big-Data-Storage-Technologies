-- Staging of one SATCAT snapshot: one row per object, business keys normalized,
-- hash keys and hashdiffs computed once for all downstream steps.
CREATE TABLE stage.satcat
ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    dv_hash(toString(norad_cat_id)) AS hk_object,
    -- launch = international designator without the piece letter: 1998-067A -> 1998-067
    if(match(ifNull(object_id, ''), '^[0-9]{4}-[0-9]{3}'), upper(substring(object_id, 1, 8)), NULL)
        AS launch_designator,
    if(launch_designator IS NULL, NULL, dv_hash(launch_designator)) AS hk_launch,
    if(launch_designator IS NULL, NULL,
       dv_hash(concat(toString(norad_cat_id), '||', launch_designator))) AS hk_object_launch,
    norad_cat_id,
    object_name,   -- already trimmed by the ODS parser (parsing.py)
    object_type,
    owner,
    orbit_center,
    ops_status_code,
    decay_date,
    data_status_code,
    orbit_type,
    rcs,
    period,
    inclination,
    apogee,
    perigee,
    launch_date,
    launch_site,
    dv_hash(concatWithSeparator('||',
        ifNull(object_name, ''), ifNull(object_type, ''), ifNull(owner, ''),
        ifNull(orbit_center, ''))) AS hd_desc,
    dv_hash(concatWithSeparator('||',
        ifNull(ops_status_code, ''), ifNull(toString(decay_date), ''),
        ifNull(data_status_code, ''), ifNull(orbit_type, ''), ifNull(toString(rcs), ''),
        ifNull(toString(period), ''), ifNull(toString(inclination), ''),
        ifNull(toString(apogee), ''), ifNull(toString(perigee), ''))) AS hd_status,
    dv_hash(concatWithSeparator('||',
        ifNull(toString(launch_date), ''), ifNull(launch_site, ''))) AS hd_launch,
    loaded_at AS load_dts,
    source_file AS record_source
FROM ods.satcat
WHERE load_id = {load_id:String}
ORDER BY norad_cat_id
LIMIT 1 BY norad_cat_id;
