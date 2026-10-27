-- check: objects_without_catalog
-- severity: warning
-- description: Объекты, известные по GP / SupGP / SOCRATES, но отсутствующие в SATCAT (нет сателлита с описанием)
SELECT norad_cat_id, record_source
FROM rv.hub_object FINAL
WHERE hk_object NOT IN (SELECT hk_object FROM rv.sat_object_satcat_desc)
