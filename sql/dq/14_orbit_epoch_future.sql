-- check: orbit_epoch_future
-- severity: warning
-- description: Наборы элементов с эпохой больше чем через сутки после загрузки (прогнозные элементы, например CXO, XMM-Newton)
SELECT 'gp' AS source, h.norad_cat_id, g.epoch, g.load_dts
FROM rv.sat_object_orbit_gp AS g
INNER JOIN rv.hub_object AS h FINAL ON h.hk_object = g.hk_object
WHERE g.epoch > g.load_dts + INTERVAL 1 DAY
