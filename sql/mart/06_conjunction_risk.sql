-- mart: conjunction_risk
-- insight: the riskiest close approaches of the current SOCRATES forecast with both objects
--          described (name, type, owner, orbit); what kinds of objects meet (debris vs
--          payload) and which objects are involved in the most conjunctions.
-- order_by: (tca, norad_cat_id_1, norad_cat_id_2)
SELECT c.norad_cat_id_1 AS norad_cat_id_1,
       ifNull(o1.object_name, toString(c.norad_cat_id_1)) AS object_name_1,
       ifNull(o1.object_type, 'UNK') AS object_type_1,
       ifNull(o1.owner, 'TBD') AS owner_1,
       c.norad_cat_id_2 AS norad_cat_id_2,
       ifNull(o2.object_name, toString(c.norad_cat_id_2)) AS object_name_2,
       ifNull(o2.object_type, 'UNK') AS object_type_2,
       ifNull(o2.owner, 'TBD') AS owner_2,
       arrayStringConcat(arraySort([object_type_1, object_type_2]), ' / ') AS pair_kind,
       ifNull(o1.is_starlink, false) OR ifNull(o2.is_starlink, false) AS involves_starlink,
       c.tca AS tca,
       c.tca_range AS tca_range_km,
       c.tca_relative_speed AS relative_speed_km_s,
       c.max_prob AS max_prob,
       c.dilution AS dilution,
       ifNull(o1.orbit_regime, o2.orbit_regime) AS orbit_regime,
       round(ifNull(o1.perigee_km, o2.perigee_km), 1) AS perigee_km,
       c.load_dts AS forecast_loaded_at
FROM bv.conjunction_current AS c
LEFT JOIN bv.object_current AS o1 ON o1.norad_cat_id = c.norad_cat_id_1
LEFT JOIN bv.object_current AS o2 ON o2.norad_cat_id = c.norad_cat_id_2
SETTINGS join_use_nulls = 1
