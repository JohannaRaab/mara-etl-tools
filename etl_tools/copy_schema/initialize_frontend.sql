CREATE EXTENSION IF NOT EXISTS cstore_fdw;

CREATE EXTENSION IF NOT EXISTS hll;

DROP AGGREGATE IF EXISTS SUM(hll);
CREATE AGGREGATE SUM(hll) (
  SFUNC     = hll_union,
  STYPE     = hll,
  FINALFUNC = hll_cardinality
);

DO $$
BEGIN
  IF NOT (SELECT exists(SELECT 1
                        FROM pg_foreign_server
                        WHERE srvname = 'cstore_server'))
  THEN CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
  END IF;
END$$;

CREATE EXTENSION IF NOT EXISTS pg_trgm;
