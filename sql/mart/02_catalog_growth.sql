-- mart: catalog_growth
-- insight: how the population of Earth orbit grew since 1957: objects catalogued and decayed
--          per year and the running total in orbit, by object type. SATCAT gives debris the
--          launch date of its parent (Fengyun-1C debris is "launched" in 1999), so an object
--          is counted from its catalogue date instead: NORAD numbers are issued in order, and
--          the latest launch date among numbers up to an object's own dates its cataloguing.
--          Breakups then appear in the year they happened (2007, 2009, 2021).
-- order_by: (year, object_type)
SELECT year,
       makeDate32(year, 1, 1) AS year_start,   -- Date32: years before 1970
       object_type,
       catalogued,
       decayed,
       sum(toInt64(catalogued) - toInt64(decayed)) OVER (
           PARTITION BY object_type ORDER BY year
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS in_orbit
FROM
(
    SELECT year, object_type, sum(catalogued) AS catalogued, sum(decayed) AS decayed
    FROM
    (
        SELECT toYear(catalog_date) AS year, object_type, 1 AS catalogued, 0 AS decayed
        FROM
        (
            SELECT object_type, orbit_center,
                   max(launch_date) OVER (ORDER BY norad_cat_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS catalog_date
            FROM bv.object_current
            WHERE launch_date IS NOT NULL
        )
        WHERE ifNull(orbit_center, 'EA') = 'EA'
        UNION ALL
        SELECT toYear(decay_date) AS year, object_type, 0 AS catalogued, 1 AS decayed
        FROM bv.object_current
        WHERE launch_date IS NOT NULL AND decay_date IS NOT NULL
          AND ifNull(orbit_center, 'EA') = 'EA'
    )
    GROUP BY year, object_type
)
