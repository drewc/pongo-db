CREATE SCHEMA IF NOT EXISTS pongo;
SET search_path TO pongo,public;

CREATE OR REPLACE FUNCTION "describe_pkey" (anyelement) 
RETURNS json AS $$
-- => json array of {"name":"","type":""} objects
-- Tell us what the pkeys columns are, and their types.
SELECT json_agg(pkeys)
 FROM (SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
         FROM   pg_index i
         JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
         WHERE  i.indrelid = pg_typeof($1)::text::regclass -- ::regclass
         AND    i.indisprimary) AS pkeys;

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "json_pkey" (anyelement) 
RETURNS json AS $$
WITH json AS (
 SELECT to_json($1)
)
  SELECT json_object_agg((foo.value)->>'name', 
                         (SELECT json.to_json-> ((foo.value)->>'name') FROM json))
  FROM json_array_elements("describe_pkey"($1)) AS foo
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION _id (anyelement) 
RETURNS json AS $$
WITH _id AS (
 SELECT json_agg(json_each.value) AS _id FROM json_each("json_pkeys"($1))
)
 SELECT CASE WHEN (json_array_length(_id) > 1)
          THEN _id ELSE _id->0 END from _id;
$$ LANGUAGE SQL;
