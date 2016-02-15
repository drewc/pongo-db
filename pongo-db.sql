
CREATE SCHEMA IF NOT EXISTS pongo;
SET search_path TO pongo,public;

CREATE OR REPLACE FUNCTION json_concat(VARIADIC jsons json[])
RETURNS json AS $$
  SELECT json_object_agg((j).key, (j).value) AS json_concat
   FROM (SELECT json_each(t) AS j
          FROM UNNEST($1) AS t          
         ) AS json_concat
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION json_remove(json json, VARIADIC keys text[])
RETURNS json AS $$
  SELECT json_object_agg(key, value) AS json_remove
   FROM json_each($1) 
   WHERE key != ALL ($2);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION json_strip_nulls(json json)
RETURNS json AS $$
  SELECT json_object_agg(key, value) AS json_remove
   FROM json_each($1) 
   WHERE $1->>key != 'null';
$$ LANGUAGE SQL;


 -- We define this here so that we can recursively use "to_sql"(json) when
 -- defining this later on.
CREATE FUNCTION "smooth_operator"(json, anyelement DEFAULT null::text)
 RETURNS text AS $$ 
 BEGIN
  RAISE EXCEPTION 'Smooth Operators not yet defined' ;
 END;
$$ language PLPGSQL;


 CREATE OR REPLACE FUNCTION to_sql(json, anyelement DEFAULT null::text)
 RETURNS text AS $$
 -- Possible types are object, array, string, number, boolean, and null.
   SELECT 
     CASE typeof 
       WHEN 'string' THEN format('%L', value)
       WHEN 'number' THEN value
       WHEN 'boolean' THEN value
       WHEN 'object' THEN pongo."smooth_operator"($1, $2)
       WHEN 'array' THEN ('ARRAY' || value)
       WHEN 'null' THEN 'NULL'
     END 
   FROM (SELECT json_typeof($1) AS typeof, 
               (json_build_object('v', $1))->>'v' AS value) 
   AS tv
 $$ language SQL;



CREATE OR REPLACE FUNCTION "%insert_columns" (json)
RETURNS text[] AS $$

SELECT array_agg(DISTINCT (t).key)
FROM (
  SELECT
    json_each(
      CASE json_typeof($1)
       WHEN 'object' THEN $1
       WHEN 'array' THEN json_array_elements($1)
      END
    ) AS t
 ) AS this ;

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "%insert_values_string_object" (json, columns text[] DEFAULT null)
RETURNS text AS $$

 SELECT '('|| string_agg(
   CASE WHEN (($1->>key) IS NOT NULL) THEN
         pongo.to_sql($1->key)
        ELSE 'DEFAULT'
        END, ', ') || ')'
 FROM UNNEST(COALESCE($2, pongo."%insert_columns"($1))) AS key ;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "%insert_values_string" (json, columns text[] DEFAULT null)
RETURNS text AS $$
 SELECT CASE json_typeof($1)
        WHEN 'object' THEN
         pongo."%insert_values_string_object"($1, $2)
        WHEN 'array' THEN
        (SELECT string_agg(pongo."%insert_values_string_object"(json, COALESCE($2, pongo."%insert_columns"($1))),
                           ', ')
                FROM json_array_elements($1) AS json)
        END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "insert_to_sql"(json)
  RETURNS text AS $$ 
   SELECT concat('INSERT INTO '||"into",
            '('||(select string_agg(key, ',') FROM unnest(columns) AS key)||')', 
            CASE 
             WHEN (($1->>'values') ILIKE 'default')
              THEN ' DEFAULT VALUES '
              ELSE ' VALUES ' ||pongo."%insert_values_string"("values", "columns")
            END, 
          ' RETURNING ' || "returning")
    FROM (SELECT $1->>'into' AS "into",
                 pongo."%insert_columns"($1->'values') AS "columns",
                 $1->'values' as "values",
                 $1->>'returning' as "returning") AS this;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "insert"(json)
RETURNS SETOF json AS $$
DECLARE
 results json[];
 foo record;
 result_oid oid;
 row_count integer;
BEGIN

 IF (($1->>'returning') IS NOT NULL) THEN
    FOR foo IN EXECUTE pongo."insert_to_sql"($1) LOOP
     RAISE NOTICE '%s', foo;
     results := results || to_json(foo);
    END LOOP;
    RETURN QUERY SELECT UNNEST(results) AS "insert";
  ELSE
   EXECUTE pongo."insert_to_sql"($1);
   GET DIAGNOSTICS result_oid := RESULT_OID, row_count := ROW_COUNT;
   RETURN QUERY SELECT json_build_object('result_oid', "result_oid", 'row_count', "row_count" );
 END IF;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "_type"(anyelement) RETURNS json AS $$
  SELECT to_json(pg_typeof($1)) AS "type" ;
$$ LANGUAGE SQL;

CREATE FUNCTION "join_to_sql"(json) RETURNS text AS $$
 SELECT 'error'::text;
$$ LANGUAGE SQL;

CREATE FUNCTION "where_to_sql"(json, text DEFAULT '', anyelement DEFAULT NULL::text) RETURNS text AS $$
 SELECT 'error'::text;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION "json_from_to_object"(json) RETURNS json AS $$
SELECT CASE json_typeof($1)
  WHEN 'object' THEN $1
  WHEN 'string' THEN json_build_object($1::text, json_build_object('as', $1))
 END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "table_from_to_sql"(json) RETURNS text AS $$
SELECT string_agg(concat(key, ' AS ' || "as"), ', ')
FROM (SELECT key, value->>'as' AS "as"
      FROM json_each(pongo."json_from_to_object"($1)))
     AS this;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "object_from_to_sql"(json) RETURNS text AS $$
SELECT string_agg(
 CASE key 
  WHEN '$select' THEN 
  CONCAT('(', pongo."to_sql"($1), ') AS ', 
         coalesce($1->>'as', 'this'))
  WHEN '$join' THEN 
   pongo.join_to_sql(value)
  ELSE ', ' || pongo."table_from_to_sql"(json_build_object(key, value))
 END, ' ')
FROM json_each(pongo."json_from_to_object"($1))
     AS this;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "from_to_sql"(json) RETURNS text AS $$
SELECT string_agg(
 CASE key 
  WHEN '$select' THEN 
  CONCAT('(', pongo."to_sql"($1), ') AS ', 
         coalesce($1->>'as', 'this'))
  WHEN '$join' THEN 
   pongo.join_to_sql(value)
  ELSE CASE WHEN (num > 1) THEN ', ' ELSE ' ' END || pongo."table_from_to_sql"(json_build_object(key, value))
 END, ' ')
FROM (SELECT key, value, ROW_NUMBER() OVER() AS num
      FROM json_each(pongo."json_from_to_object"($1))
      WHERE key != 'as')
     AS this ; 


$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION "join_to_sql"(json) RETURNS text AS $$
SELECT string_agg(
  CONCAT(value->>'join_type', ' JOIN ',
         pongo."table_from_to_sql"(json_build_object(key, value)), 
          ' ON ' || pongo."where_to_sql"(
            (SELECT json_object_agg(key, 
               CASE json_typeof(value)
                    WHEN 'string' THEN 
                     json_build_object('$eq', 
                      json_build_object('$raw', value))
                   ELSE 
                      json_build_object('$raw', value)
               END)
 
             FROM json_each($1->'on')))
  ), ' '
  ) FROM json_each($1) 
    WHERE key != 'on'
    AND key != 'as';

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "from"(anyelement) RETURNS json AS $$
   SELECT json_build_object(
              pg_typeof($1),
              json_build_object(
               'as', pg_typeof($1))
            );
  
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION "describe_pkey" (anyelement) 
RETURNS json AS $$
-- => json array of {"name":"","type":""} objects
-- Tell us what the pkeys columns are, and their types.
SELECT json_agg(pkeys)
 FROM (SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
         FROM   pg_index i
         JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
         WHERE  i.indrelid = to_regclass(pg_typeof($1)::text::cstring)
         AND    i.indisprimary) AS pkeys;

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "json_pkey" (anyelement)
RETURNS json AS $$
WITH json AS (
 SELECT to_json($1)
)
  SELECT json_object_agg((foo.value)->>'name', 
                         (SELECT json.to_json-> ((foo.value)->>'name') FROM json))
  FROM json_array_elements(pongo."describe_pkey"($1)) AS foo
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "json_pkey" (anyelement, json)
RETURNS json AS $$
  SELECT json_object_agg((foo.value)->>'name', ($2)->(foo.value)->>'name')
  FROM json_array_elements(pongo."describe_pkey"($1)) AS foo 
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION _id (anyelement) 
RETURNS json AS $$
WITH _id AS (
 SELECT json_agg(json_each.value) AS _id FROM json_each(pongo."json_pkey"($1))
)
 SELECT CASE WHEN (json_array_length(_id) > 1)
          THEN _id ELSE _id->0 END from _id;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION _id (anyelement, json, element_name text DEFAULT null) 
RETURNS json AS $$

WITH "desc" AS (
 SELECT pongo.describe_pkey($1) AS "desc"
)
 
 SELECT
   CASE json_typeof($2)
     WHEN 'array' THEN
     (SELECT json_object_agg (name, $2->(num::int - 1))
      FROM (SELECT value->>'name' AS name,
                    row_number() OVER () AS num
            FROM json_array_elements((SELECT * FROM "desc"))
            AS d)
      AS pkey)
           ELSE json_build_object(concat("element_name" || '.',
                                         ((SELECT * FROM "desc")->0)->>'name'),
                                  $2)
                         
   END ;
 
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION replace__id (anyelement, json) 
RETURNS json AS $$
 SELECT CASE WHEN (($2->'_id') IS NOT NULL) THEN
         pongo.json_concat(pongo._id($1, $2->'_id'), "object")
         ELSE "object" END
 FROM (SELECT
        json_object_agg(
         key, CASE json_typeof(value) 
               WHEN 'object' 
               THEN pongo.replace__id($1, value) 
               ELSE value 
              END) AS "object"
       FROM json_each($2) 
       WHERE key != '_id')
 AS this
 
$$ LANGUAGE SQL;

  --  \df "where_to_sql"
  --   DROP FUNCTION "where_to_sql"(json, text, variadic anyarray);
  --   DROP FUNCTION where_to_sql(json,text,anyelement);

    CREATE OR REPLACE FUNCTION "where_to_sql"
        ("object" json,
         "condition" text DEFAULT 'AND',
         "element" anyelement DEFAULT NULL::text)
    RETURNS text AS $$
    DECLARE 
      "where" text[] ;
      "text" text := '';

     -- * These are for the LOOP 
     "field" RECORD; 
 
    BEGIN
 
     IF ((json_typeof($1) = 'null')) THEN
      RETURN NULL;
     END IF;
   
--      RAISE NOTICE '%', pg_typeof("object"); 
     FOR "field" IN (SELECT key, value
                     FROM json_each("object"))
      LOOP 

      CASE 
      -- smooth operators
       WHEN (substring("field".key FROM 1 FOR 1) = '$') THEN
        where := "where" ||  pongo."smooth_operator"("field".key, "field".value, "element", "object");
       WHEN (("field".key = '_id') 
             AND (pg_typeof("element")::text != 'text')) THEN
        where :=  "where" || 
            pongo."smooth_operator"('$and', pongo._id("element", "field".value), "element");
       ELSE 
        "text" := "field".key || ' ' ;
   
        CASE (json_typeof("field".value))
         WHEN 'object' THEN 
           "text" := "text" || pongo."smooth_operator"("field".value, "element");
         WHEN 'null' THEN
           "text" := "text" || 'IS NULL';
         ELSE 
           "text" := "text" || '= ' || pongo.to_sql("field".value) ;
        END CASE;
        "where" := "where" || "text";
     
      END CASE;
      END LOOP;

    RETURN (SELECT '(' || string_agg(unnest, ' '||"condition"||' ') || ')'
            FROM (SELECT UNNEST("where")) AS this);
    END;
    $$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION "object_order_by_to_sql"(json) RETURNS text AS $$
 WITH "raw" AS (
  SELECT json_object_agg(key, CASE value::text 
                               WHEN 'true' THEN
                                json_build_object('$raw', '')
                               ELSE value END) AS this
  FROM json_each($1)
 )

 SELECT regexp_replace(pongo.where_to_sql(this, ','), '^\((.*)\)$', '\1')
   FROM "raw";
          
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "order_by_to_sql"(json) RETURNS text AS $$
 SELECT CASE json_typeof($1)
  WHEN 'object' THEN
    pongo."object_order_by_to_sql"($1)
  WHEN 'array' THEN
   (SELECT string_agg(pongo."order_by_to_sql"(jso), ', ')
    FROM json_array_elements($1) AS jso)
  WHEN 'string' THEN
   pongo.to_sql(json_build_object('$raw', $1))
 END
          
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "select_expression_to_sql"("expression" text, "select" json)
RETURNS text AS $$
 
SELECT concat(
-- [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
--     [ * | expression [ [ AS ] output_name ] [, ...] ]
 'SELECT '||"expression", 
 -- [ FROM from_item [, ...] ]
  ' FROM ' || "from",
 -- [ WHERE condition ]
  ' WHERE ' || "where",
       -- [ GROUP BY expression [, ...] ]
       -- [ HAVING condition [, ...] ]
       -- [ WINDOW window_name AS ( window_definition ) [, ...] ]
       -- [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
 -- [ ORDER BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ]
  ' ORDER BY ' || "order_by",
 -- [ LIMIT { count | ALL } ]
  ' LIMIT ' || "limit",
        --  [ OFFSET start [ ROW | ROWS ] ]
  ' OFFSET ' || "offset"
        -- [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]
        -- [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT ] [...] ]
 )
FROM (SELECT pongo."from_to_sql"($2->'from') AS "from",
             pongo."where_to_sql"($2->'where') AS "where", 
             pongo."order_by_to_sql"($2->'order_by') AS "order_by",
             CASE $2->>'limit' WHEN 'ALL' THEN $2->>'limit' ELSE ($2->>'limit') END AS "limit", 
             $2->'offset' AS "offset")
AS this;

 

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "select_to_sql"("select" json)
RETURNS text AS $$
 SELECT CASE json_typeof($1)
         WHEN 'object' THEN
          pongo."select_expression_to_sql"((SELECT coalesce($1->>'expression', '*')), $1)
         WHEN 'array' THEN
          pongo."select_expression_to_sql"(
                (SELECT string_agg(json_build_object('v',value)->>'v', ', ')
                 FROM json_array_elements($1)
                 WHERE json_typeof(value) = 'string'),
                  CASE json_typeof($1->(json_array_length($1) - 1))
                   WHEN 'object' THEN
                    $1->(json_array_length($1) - 1)
                   ELSE NULL
                  END
             )
          END;

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "select_record"("select" json)
RETURNS setof record AS $$
BEGIN
RETURN QUERY EXECUTE pongo."select_to_sql"($1) ;
END;
$$ LANGUAGE plpgSQL;

CREATE OR REPLACE FUNCTION "select"("select" json)
RETURNS setof json AS $$
BEGIN
 RETURN QUERY EXECUTE 'SELECT to_json("record".*) AS "select" FROM (' ||
                      pongo."select_to_sql"($1) || ') AS "record"';
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION "smooth_operator"(text)
RETURNS text AS $$
 SELECT 
  CASE $1
   WHEN '$raw' THEN ''
   WHEN '$eq' THEN '='
   WHEN '$gt' THEN '>'
   WHEN '$gte' THEN '>='
   WHEN '$lt' THEN '<'
   WHEN '$lte' THEN '<='
   WHEN '$ne'  THEN '!='
   WHEN '$not' THEN 'NOT'
   WHEN '$is' THEN 'IS'
   WHEN '$like' THEN 'LIKE'
   WHEN '$ilike' THEN 'ILIKE'
   WHEN '$regex' THEN '~'
   WHEN '$regex*' THEN '~*'
   ELSE 'Error: Nothing for "' || $1 ||'"'
 END 
$$ LANGUAGE sql ;


  CREATE OR REPLACE FUNCTION "smooth_operator"(json, anyelement DEFAULT null::text)
  RETURNS text AS $$
   SELECT pongo."smooth_operator"(key, value, $2) AS "$mooth Operator"
          FROM json_each($1) AS json;
  $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION "smooth_operator"("key" TEXT, "value" json, "element" anyelement, "object" json)
  RETURNS text AS $$
   SELECT
     text(CASE key 
          WHEN '$any' THEN concat('ANY ', pongo.to_sql(value))
          WHEN '$or' THEN pongo."where_to_sql"(value, 'OR', $3)
          WHEN '$and' THEN pongo."where_to_sql"(value, 'AND', $3)
          WHEN '$select' THEN pongo."select_to_sql"(value)
       --    WHEN '$not' THEN concat('NOT ', pongo."where_to_sql"(value, 'AND', $3))
          WHEN '$from' THEN pongo."from_to_sql"(value)
          WHEN '$order_by' THEN pongo."order_by_to_sql"(value)
          WHEN '$raw' THEN (json_build_object('v', value))->>'v'
          WHEN '$asc' THEN
            CASE value::text
             WHEN 'true' THEN 'ASC' 
             ELSE concat(pongo.to_sql(value) || ' ASC',
                         CASE "object"->>'nulls'
                          WHEN 'first' THEN 'NULLS FIRST'
                          WHEN 'last' THEN 'NULLS LAST'
                         END)
            END  
          WHEN '$desc' THEN
            CASE value::text
             WHEN 'true' THEN 'DESC' 
             ELSE concat(pongo.to_sql(value) || ' DESC',
                         CASE "object"->>'nulls'
                          WHEN 'first' THEN 'NULLS FIRST'
                          WHEN 'last' THEN 'NULLS LAST'
                         END)
            END
      
          WHEN '$nulls_first' THEN
            CASE value::text
             WHEN 'true' THEN 'NULLS FIRST'
             ELSE pongo.to_sql(value) || ' NULLS FIRST'
            END
          WHEN '$nulls_last' THEN
            CASE value::text
             WHEN 'true' THEN 'NULLS LAST'
             ELSE pongo.to_sql(value) || ' NULLS LAST'
            END
          ELSE (pongo."smooth_operator"(key) 
            ||' '||pongo.to_sql(value))
          END) AS "$mooth Operator"
  $$ LANGUAGE sql;

  CREATE OR REPLACE FUNCTION "smooth_operator"("key" TEXT, "value" json, anyelement DEFAULT null::text)
  RETURNS text AS $$
   SELECT pongo.smooth_operator($1, $2, $3, NULL::json);
  $$ LANGUAGE sql;





CREATE OR REPLACE FUNCTION "DTRT_with__id_and__type_for_inclusive"(projection json)
RETURNS json AS $$
  SELECT pongo.json_concat(
    CASE WHEN ((($1->>'_id') IS NOT NULL)) THEN NULL
         ELSE json_build_object('_id', true)
    END, 
    CASE WHEN ((($1->>'_type') IS NOT NULL))
         THEN NULL ELSE json_build_object('_type', true)
    END, 
    $1
  );

$$ LANGUAGE SQL;
 
CREATE OR REPLACE FUNCTION "view"(jso json, projection json)
 RETURNS json AS $$

 SELECT 
 -- We see if it's include or exclude
    CASE COALESCE (
           (SELECT CASE 
                    WHEN (value = '1' OR value = 'true') THEN
                     'include'
                    WHEN (value = '0' OR value = 'false') THEN 
                     'exclude'
                   END
           FROM (SELECT json.value AS value
                 FROM json_each_text("projection") AS json 
                 WHERE substring(json.key FROM 1 FOR 1) != '$' 
                 AND key != '_id'::text
                 AND key != '_type'::text
                 LIMIT 1) 
           AS include_or_exclude), 
          (SELECT 'exclude'::text FROM json_each("projection") LIMIT 1))::text
   -- include
     WHEN 'include' THEN
       (SELECT json_object_agg((json).key, "jso"->(json).key) AS json
        FROM (SELECT json_each_text (
              pongo."DTRT_with__id_and__type_for_inclusive"(projection)) AS json) 
        AS json_each
        WHERE substring((json).key FROM 1 FOR 1) != '$'
              AND CASE WHEN (((json).key = '_id') OR ((json).key = '_type'))
                       THEN CASE WHEN ((json_typeof((COALESCE("jso"->(json).key, 
                                                   json_build_object ('', NULL))
                                         )->'') = 'null') 
                                       OR "projection"->>(json).key = '0'
                                       OR "projection"->>(json).key = 'false')
                                 THEN false 
                                 ELSE true 
                                 END
                   ELSE true END)
                    
   -- exclude
     WHEN 'exclude' THEN
       (SELECT json_object_agg((json).key, (json).value) AS json
        FROM (SELECT json_each ("jso") AS json) 
        AS json_each
        WHERE (projection->(json).key) IS NULL)
    ELSE "jso"
    END

 $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION default_as_json_options()
RETURNS json AS $$
 SELECT '{ 
           "view" : null,
           "_type" : true,
           "_id"  : true,
           "null" : false,
           "primary_key" : false
         }'::json
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION default_as_json_options(anyelement)
RETURNS json AS $$
 SELECT pongo.default_as_json_options ();
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION default_as_json(anyelement,                                   
                                              "options" json DEFAULT NULL)
RETURNS json AS $$
-- First, to_json
  WITH to_json AS (
    SELECT to_json($1) AS to_json
-- options, each either supplied or default    
  ), "options" AS (
    SELECT json_object_agg(key, coalesce($2->key, value)) AS "options"
     FROM json_each(pongo.default_as_json_options($1))
  ), "id" AS (
    SELECT CASE WHEN (SELECT "options"->>'_id' = 'true' FROM "options") THEN
                  pongo.json_concat(json_build_object('_id', pongo._id($1)), to_json)
                ELSE to_json
           END AS "id"
    FROM to_json AS "id"
  ), "type" AS (
    SELECT CASE WHEN (SELECT "options"->>'_type' = 'true' FROM "options") THEN
                  pongo.json_concat(json_build_object('_type', pongo._type($1)), "id")
                ELSE "id"
           END AS "type"
    FROM "id"
  ), "pkey" AS (
   SELECT CASE WHEN (SELECT "options"->>'primary_key' = 'false' FROM "options") THEN
              (SELECT json_object_agg(key, value)
                FROM json_each((SELECT "type" FROM "type"))
                WHERE ((pongo.json_pkey($1)->key) IS NULL))
            ELSE (SELECT "type" FROM "type")
           END AS "pkey"

  ), "null" AS (
   SELECT CASE WHEN (SELECT "options"->>'null' = 'false' FROM "options") THEN
                pongo.json_strip_nulls("pkey")
               ELSE "pkey"
                   
           END AS "null"
     FROM "pkey"
  ), "this" AS (
   SELECT "null" AS "this"  FROM "null"
  )
                   
  SELECT CASE WHEN (SELECT "options"->>'view' != 'null' FROM "options") THEN
               pongo."view"("this", (SELECT "options"->'view'))
              ELSE "this"
              END
  FROM "this"
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION as_json(anyelement, "options" json DEFAULT NULL)
RETURNS json AS $$
 SELECT pongo.default_as_json($1, $2) ;
$$ LANGUAGE SQL;



  CREATE OR REPLACE FUNCTION "default_find_query"()
  RETURNS json AS $$ 
   SELECT  '
    {"_type" : null,
     "view" : null, 
     "where" : null,
     "single" : false,
     "paged" : false,
     "limit" : 10, 
     "offset" : 0, 
     "order_by" : false, 
     "as_json" : null

     }'::json 
  $$ LANGUAGE SQL;

  CREATE OR REPLACE FUNCTION "default_find_query"(anyelement)
  RETURNS json AS $$ 
   SELECT pongo."default_find_query"();
  $$ LANGUAGE SQL;

  CREATE OR REPLACE FUNCTION "_find_json_agg"(find_select_query json)
  RETURNS json AS $$
   SELECT json_agg(t.json) 
    FROM pongo."select_record"($1)
         AS t(json json);
    $$ LANGUAGE SQL;

  CREATE OR REPLACE FUNCTION "_find_single"(find_select_query json)
  RETURNS json AS $$
    SELECT json FROM pongo."select_record"($1)
         AS t(json json);
    $$ LANGUAGE SQL;

  CREATE OR REPLACE FUNCTION "_find_paged"(find json, find_select_query json)
  RETURNS json AS $$

   WITH count AS (
       SELECT count,
             ($2->>'limit')::bigint as limit,
             ($2->>'offset')::bigint as offset
       FROM pongo."select_record"(pongo.json_concat(
              '{"expression" : "count(*)"}',
              pongo.json_remove($2, 'limit', 'offset', 'expression', 'order_by')
            ))
       AS t(count bigint)

   ), "results" AS (
    SELECT CASE ($1->>'single')
      WHEN 'true' THEN
       pongo."_find_single"($2)
      ELSE
       pongo."_find_json_agg"($2)
      END AS "results"
   ), "this_page" AS (
     SELECT pongo.json_concat($1)
 
   ), "next_page" AS (
     SELECT CASE
       WHEN ((SELECT "count" > ("limit" + "offset") FROM count)) THEN
       pongo.json_concat(
        json_build_object('offset', (select "offset" + "limit" FROM "count")),
        pongo.json_remove($1, 'offset'))
      END
   ), "prev_page" AS (
     SELECT CASE
       WHEN (SELECT "offset" > 0 FROM "count") THEN
       pongo.json_concat(
        json_build_object('offset', (select CASE WHEN (("offset" - "limit") > 0) THEN
                                                  ("offset" - "limit")
                                                 ELSE
                                                  0
                                             END
                                     FROM "count")),
        pongo.json_remove($1, 'offset'))
      END
   ), "query_info" AS (
    SELECT json_build_object(
     'page_size', (SELECT CASE WHEN ($1->>'single' = 'true') THEN
                               1
                               WHEN ("limit" > "count")
                               THEN "count"
                               ELSE (SELECT json_array_length("results") FROM "results")
                          END
                   FROM "count"),
     'page_start_offset', (SELECT "offset" FROM "count"),
     'total_results', (SELECT "count" FROM "count"),
     'this_page', (SELECT * FROM "this_page"),
     'next_page', (SELECT * FROM "next_page"),
     'prev_page', (SELECT * FROM "prev_page")
   ))


   SELECT json_build_object(
   'result_page', (select * from "results"),
   'query_info', (select * from query_info)
                       );

  $$ LANGUAGE SQL;


  CREATE OR REPLACE FUNCTION "find_type"("type" anyelement, find json)
  RETURNS json AS $$
  DECLARE 
   "name" text;
   "from" json;
   "expression" text;
   "default_query" json;
   "single" bool;
   "paged" bool;
   "limit" json;
   "offset" json;
   "order_by" json;
   "find_select_query" json;
   "as_json" json;
  BEGIN

  -- * Type

    "name" :=  pg_typeof($1);
    "from" := coalesce($2->'from', pongo.from($1));
     
    --    RAISE EXCEPTION 'FROM % % %', "from", $2->'from', pongo.from($1);

    "default_query" := pongo.default_find_query($1);

    "as_json" := coalesce($2->'as_json', "default_query"->'as_json');

    "expression" := concat('pongo.as_json(CAST (', 
                           COALESCE("from"->"name"->'as',
                                    "from"->'as'), '.* AS ', name,')',
                            ', ') ;

--      RAISE EXCEPTION 'EXPRESSION % % ', "from"->'as', "expression";
    "expression" := "expression" || 'pongo.json_concat(';
     IF (("as_json" IS NOT NULL)
                 AND "as_json"::text != 'null') THEN
     "expression" := expression || format('%L',"as_json") || '::json';
     ELSE "expression" := expression || 'null::json';
     END IF;
     IF (($2->>'view') IS NOT NULL
                AND ($2->>'view') != 'null') THEN
     "expression" := expression || 
              ', json_build_object(''view'', json(' ||
               format('%L',$2->>'view')|| '))' ;
     END IF;

     expression := expression || '))'; 
  -- * paged
   IF (((coalesce($2->>'paged', "default_query"->>'paged'))
                    = 'true')) THEN
    "paged" := true;
  ELSE
    "paged":= false;
  END IF;

  -- * single
   IF (((coalesce($2->>'single', "default_query"->>'single'))
                    = 'true')) THEN
    "single" := true;
  ELSE
    "single":= false;
  END IF;

  -- * limit

  CASE WHEN "single" THEN
        "limit" := to_json(1) ;
       ELSE "limit" := coalesce($2->'limit', "default_query"->'limit');
  END CASE;

  -- * Offset

   "offset" := coalesce(($2->>'offset')::integer, 0);

  --  RAISE EXCEPTION 'offset %', "offset";

  -- * ORDER BY

   "order_by" := coalesce(($2->'order_by'), "default_query"->'order_by');

  -- * Quild the Query
   "find_select_query" := json_build_object(
           'expression', "expression",
           'from', "from", 
           'where', pongo.replace__id("type", $2->'where'),
           'limit', "limit",
           'offset', "offset", 
           'order_by', "order_by");

  CASE
   WHEN ("paged" = true) THEN
    RETURN pongo."_find_paged"($2, "find_select_query");
   WHEN ("single" = true) THEN
    RETURN pongo."_find_single"("find_select_query");
   ELSE
    RETURN pongo."_find_json_agg"("find_select_query");
  END CASE;
 

  END;
  $$ LANGUAGE PLPGSQL;

  CREATE OR REPLACE FUNCTION "find"(find json)
  RETURNS json AS $$ 
  DECLARE 
   "result" json;
  BEGIN

  -- * Type

   IF (($1->'_type') IS NOT NULL) THEN
     EXECUTE concat('SELECT pongo.find_type(CAST(NULL AS ', $1->'_type', '), $1)')
      INTO "result" using $1;
      RETURN "result";
   ELSE
    RAISE EXCEPTION '_type must be provided for find()';
   END IF;

  END;
  $$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION "update_set_to_sql"(json) RETURNS text AS $$
 SELECT regexp_replace(pongo.where_to_sql($1, ','), '^\((.*)\)$', '\1') ;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "update_where_to_sql"(json) RETURNS text AS $$
 SELECT pongo.where_to_sql($1) ;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "update_to_sql" (anyelement, json)
RETURNS text AS $$ 
SELECT concat('UPDATE '||CASE pg_typeof($1)::text
                          WHEN 'text' THEN $1::text ELSE pg_typeof($1)::text END, 
             ' SET ' ||pongo."update_set_to_sql"($2->'set'),
             ' WHERE ' || pongo."update_where_to_sql"($2->'where'),
             ' RETURNING ' || (SELECT $2->>'returning')) ;
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION "update"(anyelement, json)
RETURNS SETOF json AS $$
DECLARE
 results json[];
 foo record;
 result_oid oid;
 row_count integer;
BEGIN

 IF (($2->>'returning') IS NOT NULL) THEN
    FOR foo IN EXECUTE pongo."update_to_sql"($1, $2) LOOP
     RAISE NOTICE '%s', foo;
     results := results || to_json(foo);
    END LOOP;
    RETURN QUERY SELECT UNNEST(results) AS "update";
  ELSE
   EXECUTE pongo."update_to_sql"($1, $2);
   GET DIAGNOSTICS result_oid := RESULT_OID, row_count := ROW_COUNT;
   RETURN QUERY SELECT json_build_object('result_oid', "result_oid", 'row_count', "row_count" );
 END IF;

END;
$$ LANGUAGE plpgsql;
