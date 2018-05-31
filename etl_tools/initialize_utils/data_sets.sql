-- creation of attribute lookup tables for data sets

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- gets attributes from data set (text fields)
CREATE OR REPLACE FUNCTION util.get_data_set_attributes(param_schema_name TEXT, param_table_name TEXT)
  RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY
  SELECT
    column_name
  FROM information_schema.columns
  WHERE table_schema = param_schema_name
        AND table_name = param_table_name
        AND data_type IN ('text', 'varchar');
END $$ LANGUAGE plpgsql;

-- creates a table optimized for the auto-completion of data set attributes
CREATE OR REPLACE FUNCTION util.create_data_set_attributes_table(param_schema_name TEXT, param_table_name TEXT)
  RETURNS VOID AS $$
DECLARE v_column_name TEXT;
BEGIN
  EXECUTE 'DROP TABLE IF EXISTS ' || param_schema_name || '.' || param_table_name || '_attributes';
  EXECUTE 'CREATE TABLE ' || param_schema_name || '.' || param_table_name ||
          '_attributes (attribute TEXT NOT NULL, value TEXT NOT NULL);';

  FOR v_column_name IN util.get_data_set_attributes(param_schema_name, param_table_name) LOOP
    EXECUTE 'INSERT INTO ' || param_schema_name || '.' || param_table_name || '_attributes ' ||
            'SELECT DISTINCT ''' || v_column_name || ''', "' || v_column_name ||
            '" FROM ' || param_schema_name || '.' || param_table_name ||
            ' WHERE "' || v_column_name || '" IS NOT NULL ORDER BY "' || v_column_name || '"';
  END LOOP;

  EXECUTE 'CREATE INDEX ' || param_table_name || '_attributes__attribute ON ' ||
          param_schema_name || '.' || param_table_name || '_attributes (attribute)';

  EXECUTE 'CREATE INDEX ' || param_table_name || '_attributes__value ON ' ||
          param_schema_name || '.' || param_table_name || '_attributes USING GIN (value gin_trgm_ops)';
END;
$$ LANGUAGE plpgsql;

-- the same as util.create_data_set_attributes_table but changed for parallel execution
-- before
CREATE OR REPLACE FUNCTION util.create_attributes_table_for_data_set(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'DROP TABLE IF EXISTS ' || schema_name || '.' || table_name || '_attributes';
  EXECUTE 'CREATE TABLE ' || schema_name || '.' || table_name ||
          '_attributes (attribute TEXT NOT NULL, value TEXT NOT NULL);';
END;
$$ LANGUAGE plpgsql;

-- parallel
CREATE OR REPLACE FUNCTION util.insert_column_values_into_data_set_attributes_table(schema_name TEXT, table_name TEXT,
                                                                                    column_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'INSERT INTO ' || schema_name || '.' || table_name || '_attributes ' ||
          'SELECT DISTINCT ''' || column_name || ''', "' || column_name ||
          '" FROM ' || schema_name || '.' || table_name ||
          ' WHERE "' || column_name || '" IS NOT NULL ORDER BY "' || column_name || '"';
END;
$$ LANGUAGE plpgsql;

-- after
CREATE OR REPLACE FUNCTION util.index_data_set_attributes_table(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'CREATE INDEX ' || table_name || '_attributes__attribute ON ' ||
          schema_name || '.' || table_name || '_attributes (attribute)';

  EXECUTE 'CREATE INDEX ' || table_name || '_attributes__value ON ' ||
          schema_name || '.' || table_name || '_attributes USING GIN (value gin_trgm_ops)';
END;
$$ LANGUAGE plpgsql;

-- parallized indexing
CREATE OR REPLACE FUNCTION util.index_data_set_attributes_table(schema_name TEXT, table_name TEXT, attribute TEXT)
  RETURNS void AS $$
BEGIN
  EXECUTE 'CREATE INDEX "' || table_name || '_' || attribute || '" ON ' || schema_name || '.'
          || table_name || '_attributes USING GIN (value gin_trgm_ops) WHERE attribute = ''' || attribute || '''';
END
$$ LANGUAGE plpgsql;

