-- Read-only ClickHouse user for BI (Superset) and the 3D globe: sees only the marts.
-- readonly = 2 forbids writes but lets clients set query settings (the drivers need it).
CREATE USER IF NOT EXISTS superset IDENTIFIED WITH sha256_password BY 'superset'
    SETTINGS readonly = 2;

GRANT SELECT ON mart.* TO superset;
