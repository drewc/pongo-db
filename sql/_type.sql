CREATE SCHEMA IF NOT EXISTS pongo;
SET search_path TO pongo,public;

CREATE OR REPLACE FUNCTION "_type"(anyelement) RETURNS json AS $$
  SELECT to_json(pg_typeof($1)) AS "type" ;
$$ LANGUAGE SQL;
