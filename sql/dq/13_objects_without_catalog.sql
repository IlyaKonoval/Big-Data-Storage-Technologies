-- check: objects_without_catalog
-- severity: warning
-- description: Objects known from GP / SupGP / SOCRATES but absent from SATCAT (no description satellite)
SELECT norad_cat_id, record_source
FROM rv.hub_object FINAL
WHERE hk_object NOT IN (SELECT hk_object FROM rv.sat_object_satcat_desc)
