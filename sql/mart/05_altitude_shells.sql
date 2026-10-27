-- mart: altitude_shells
-- insight: where low Earth orbit is crowded: objects per 25 km perigee band x 5 degree
--          inclination band (LEO, perigee below 2000 km). Starlink shells (~550 km at 53,
--          43, 70 and 97 deg) and the sun-synchronous belt (97-99 deg) stand out.
-- order_by: (altitude_km, inclination_deg, object_type)
SELECT toUInt32(floor(perigee_km / 25) * 25) AS altitude_km,
       toUInt32(floor(inclination / 5) * 5) AS inclination_deg,
       object_type,
       count() AS objects,
       countIf(is_starlink) AS starlink,
       countIf(object_type = 'DEB') AS debris
FROM bv.object_current
WHERE in_orbit AND orbit_regime = 'LEO'
  AND perigee_km BETWEEN 100 AND 2000 AND inclination BETWEEN 0 AND 180
GROUP BY altitude_km, inclination_deg, object_type
