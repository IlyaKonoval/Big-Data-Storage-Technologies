INSERT INTO rv.link_object_launch (hk_object_launch, hk_object, hk_launch, load_dts, record_source)
SELECT assumeNotNull(hk_object_launch), hk_object, assumeNotNull(hk_launch), load_dts, record_source
FROM stage.satcat
WHERE hk_object_launch IS NOT NULL
  AND hk_object_launch NOT IN (SELECT hk_object_launch FROM rv.link_object_launch);
