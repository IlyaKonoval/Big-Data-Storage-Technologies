-- mart: launch_activity
-- insight: launch cadence per year, owner and launch site; how much of the recent growth
--          is Starlink (launches and payloads), and how many objects a launch leaves behind.
--          A launch is attributed to the owner of its primary object (lowest NORAD ID).
-- order_by: (year, owner, launch_site)
SELECT toYear(l.first_date) AS year,
       makeDate32(year, 1, 1) AS year_start,   -- Date32: years before 1970
       l.owner AS owner,
       l.site AS launch_site,
       ifNull(s.site_name, l.site) AS launch_site_name,
       count() AS launches,
       sum(l.objects) AS objects,
       sum(l.payloads) AS payloads,
       countIf(l.starlink_payloads > 0) AS starlink_launches,
       sum(l.starlink_payloads) AS starlink_payloads,
       sum(l.in_orbit) AS objects_in_orbit
FROM
(
    SELECT launch_designator,
           any(launch_date) AS first_date,   -- not "launch_date": the alias would shadow the column in WHERE
           any(launch_site) AS site,
           argMin(owner, norad_cat_id) AS owner,
           count() AS objects,
           countIf(object_type = 'PAY') AS payloads,
           countIf(object_type = 'PAY' AND is_starlink) AS starlink_payloads,
           countIf(in_orbit) AS in_orbit
    FROM bv.object_current
    WHERE launch_designator IS NOT NULL AND launch_date IS NOT NULL
    GROUP BY launch_designator
) AS l
LEFT JOIN (SELECT code, name AS site_name FROM rv.ref_launch_site FINAL) AS s
       ON s.code = l.site
GROUP BY year, owner, launch_site, launch_site_name
SETTINGS join_use_nulls = 1
