-- mart: chronicle
-- insight: every object ever catalogued in Earth orbit with the time span it spent there,
--          for the 1957-2026 time-lapse on the globe: the sky fills up launch by launch, and
--          breakups (Fengyun-1C 2007, Iridium x Cosmos 2009, Cosmos 1408 2021) flash in as
--          clouds of debris in the year they happened. Appearance uses the catalogue date
--          estimated from the NORAD number (see catalog_growth); years are fractional.
--          Orbit size and inclination are the last known ones (GP, else SATCAT).
-- order_by: (appear_year, norad_cat_id)
SELECT norad_cat_id,
       object_type,
       round(toYear(catalog_date) + (toDayOfYear(catalog_date) - 1) / 365.25, 3) AS appear_year,
       if(decay_date IS NULL, NULL,
          round(toYear(decay_date) + (toDayOfYear(decay_date) - 1) / 365.25, 3)) AS decay_year,
       round(inclination, 1) AS inclination,
       toUInt32(greatest(perigee_km, 100)) AS perigee_km,
       toUInt32(greatest(apogee_km, perigee_km, 100)) AS apogee_km
FROM
(
    SELECT norad_cat_id, object_type, orbit_center, decay_date, inclination, perigee_km,
           apogee_km,
           max(launch_date) OVER (ORDER BY norad_cat_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS catalog_date
    FROM bv.object_current
    WHERE launch_date IS NOT NULL
)
WHERE ifNull(orbit_center, 'EA') = 'EA'
  AND inclination IS NOT NULL AND perigee_km IS NOT NULL AND apogee_km IS NOT NULL
