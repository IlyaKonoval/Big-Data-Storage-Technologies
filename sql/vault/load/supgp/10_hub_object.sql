INSERT INTO rv.hub_object (hk_object, norad_cat_id, load_dts, record_source)
SELECT hk_object, norad_cat_id, load_dts, record_source
FROM stage.supgp
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object);
