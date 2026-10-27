-- mart: orbit_population
-- insight: what is in orbit now: objects by orbit regime, type and owner; share of debris
--          and of inactive payloads ("dead" satellites) in each regime.
-- order_by: (orbit_regime, object_type, owner)
SELECT ifNull(orbit_regime, 'unknown') AS orbit_regime,
       object_type,
       object_type_name,
       owner,
       count() AS objects,
       countIf(object_type = 'PAY' AND is_active) AS active_payloads,
       countIf(object_type = 'PAY' AND NOT is_active) AS inactive_payloads,
       countIf(object_type = 'DEB') AS debris,
       countIf(is_starlink) AS starlink,
       round(avg(perigee_km), 1) AS avg_perigee_km,
       any(snapshot_date) AS snapshot_date
FROM bv.object_current
WHERE in_orbit
GROUP BY orbit_regime, object_type, object_type_name, owner
