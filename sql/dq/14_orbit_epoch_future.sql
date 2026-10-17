-- check: orbit_epoch_future
-- severity: warning
-- description: Element sets with epoch more than 1 day after the download (predicted elements, e.g. CXO, XMM-Newton)
SELECT 'gp' AS source, h.norad_cat_id, g.epoch, g.load_dts
FROM rv.sat_object_orbit_gp AS g
INNER JOIN rv.hub_object AS h FINAL ON h.hk_object = g.hk_object
WHERE g.epoch > g.load_dts + INTERVAL 1 DAY
