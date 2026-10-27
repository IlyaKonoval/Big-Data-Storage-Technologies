-- mart: launch_sites
-- insight: the world's spaceports on a map: launches and objects per site, first and last
--          year of use, how many of their objects are still in orbit.
-- order_by: code
SELECT s.code AS code,
       s.name AS name,
       s.country AS country,
       s.latitude AS latitude,
       s.longitude AS longitude,
       uniqExact(o.launch_designator) AS launches,
       count() AS objects,
       countIf(o.object_type = 'PAY') AS payloads,
       countIf(o.in_orbit) AS objects_in_orbit,
       countIf(o.in_orbit AND o.object_type = 'PAY' AND o.is_active) AS active_payloads,
       min(toYear(o.launch_date)) AS first_year,
       max(toYear(o.launch_date)) AS last_year
FROM bv.object_current AS o
INNER JOIN rv.ref_launch_site AS s FINAL ON s.code = o.launch_site
GROUP BY code, name, country, latitude, longitude
