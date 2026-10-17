INSERT INTO rv.hub_launch (hk_launch, launch_designator, load_dts, record_source)
SELECT assumeNotNull(hk_launch), assumeNotNull(launch_designator), any(load_dts), any(record_source)
FROM stage.satcat
WHERE hk_launch IS NOT NULL
  AND hk_launch NOT IN (SELECT hk_launch FROM rv.hub_launch)
GROUP BY hk_launch, launch_designator;
