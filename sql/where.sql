-- We define this here so that we can recursively use "sql"(json) when
-- defining this later on.
CREATE FUNCTION "smooth_operator"(json)
RETURNS text AS $$ SELECT 'error'::text $$ language SQL;

CREATE OR REPLACE FUNCTION "sql"(json)
RETURNS text AS $$
  SELECT 
    CASE typeof 
      WHEN 'string' THEN format('%L', value)
      WHEN 'number' THEN value
      WHEN 'boolean' THEN value
      WHEN 'object' THEN "smooth_operator"($1)
      WHEN 'array' THEN ('ARRAY' || value)
      ELSE 'error'
    END 
  FROM (SELECT json_typeof($1) AS typeof, 
              (json_build_object('v', $1))->>'v' AS value) 
  AS tv
$$ language SQL;



CREATE OR REPLACE FUNCTION "smooth_operator"(text)
RETURNS text AS $$
 SELECT 
  CASE $1
   WHEN '$eq' THEN '='
   WHEN '$gt' THEN '>'
   WHEN '$gte' THEN '>='
   WHEN '$lt' THEN '<'
   WHEN '$lte' THEN '<='
   WHEN '$ne'  THEN '!='
   WHEN '$not' THEN 'NOT'
   ELSE NULL
 END 
$$ LANGUAGE sql ;

-- We define this here so that we can recursively use "smooth_operator"(json) when
-- defining this later on.
CREATE FUNCTION "where"(json, unknown)
RETURNS text AS $$ SELECT 'error'::text $$ language SQL;

CREATE OR REPLACE FUNCTION "smooth_operator"(json)
RETURNS text AS $$
 SELECT 
   CASE keys.key 
    WHEN '$any' 
     THEN concat('ANY ', "sql"(keys.value))
    WHEN '$or'
     THEN concat("where"(keys.value, 'OR'))
    WHEN '$regex'
     THEN concat((CASE $1->>'$case_sensitive'::text
                  WHEN 'false' THEN '~* ' ELSE '~ ' END),
                  "sql"(keys.value))
    WHEN '$regex*'
     THEN concat('~* ', "sql"(keys.value))
    WHEN '$nregex*'
     THEN concat('!~* ', "sql"(keys.value))
    WHEN '$nregex'
     THEN concat((CASE $1->>'$case_sensitive'::text
                  WHEN 'false' THEN '!~* ' ELSE '!~ ' END),
                  "sql"(keys.value))
    ELSE ("smooth_operator"(keys.key) 
          ||' '||"sql"(keys.value))
    END
  FROM (SELECT (json).key AS key, (json).value AS value 
        FROM json_each($1) AS json) 
  AS keys
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION "select"(expression text, "from" text, "where" text)
RETURNS text AS $$
 SELECT '(SELECT '||expression||' FROM '||"from"||' WHERE '||"where"||' )'
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "select" ("select" json)
RETURNS text AS $$
SELECT "select"("select"->>'expression', "select"->>'from', "select"->>'where')
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION "where"
    (complex_query json,
     "condition" text DEFAULT 'AND',
     "element_name" text DEFAULT NULL,
     "_id" TEXT DEFAULT NULL)
RETURNS text AS $$
DECLARE 
  "where" text := 'true';

 -- * These are for the LOOP 
 "pair" RECORD; 
 
BEGIN
--  RAISE EXCEPTION 'c %', "condition";
  IF ("element_name" IS NOT NULL) THEN
   "element_name" := format('%I', "element_name");
  END IF;

  FOR "pair" IN (SELECT (json).key AS key, (json).value AS value 
                FROM (SELECT json_each("complex_query") AS json) AS json)
  LOOP 
   IF (("pair".key)::text = '$or') 
    THEN "where" := concat("where", ' '||"condition"||' ', "where"("pair".value, 'OR'));
   ELSIF (("pair".key)::text = '$and') 
    THEN "where" := concat("where", ' '||"condition"||' ', "where"("pair".value, 'AND'));
     ELSIF (("pair".key)::text = '_type')
    THEN NULL;
   ELSIF (("pair".key)::text = '_id') 
   THEN "where" := concat("where", ' '||"condition"||' ',
                          "_id",
                          CASE json_typeof("pair".value)
                              WHEN 'object'  THEN ' ' ELSE ' = ' END
                           || "sql"("pair".value)   ||')');
   ELSE
   -- Now we can set the WHERE using "sql_value"
    "where" := concat("where", 
     -- First, the condition that we passed
     ' '||"condition"||' (' 
     -- If there is a name, add it and a dot
     ||  concat(("element_name"||'.'),
     -- Now the key as an SQL identifier or a smooth key
      CASE WHEN substring("pair".key FROM 1 FOR 10) = '{"$select"'
            THEN "select"((CAST ("pair".key AS json))->'$select')
           WHEN (("pair".key)::text = '_id')
            THEN '_id('||"element_name"
 
       ELSE format('%I', "pair".key)
      END
   
      -- and finally the value

      || CASE json_typeof("pair".value)
               WHEN 'object'  THEN ' ' ELSE ' = ' END
               || "sql"("pair".value)   ||')'
            )
     );
  END IF;
  END LOOP;

  IF ("where" = 'true') THEN
    RETURN 'true';
  ELSE
    RETURN '('||COALESCE(substring("where" FROM '^true '||"condition"|| ' (.*)')||')', 
                        "where");
  END IF;



END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION "where"
    (anyelement, 
     complex_query json,
     "condition" text DEFAULT 'AND',
     "element_name" text DEFAULT NULL,
     "_id" text DEFAULT NULL
     )
RETURNS text AS $$
 SELECT "where"($2, $3,
                COALESCE($4, pg_typeof($1)::text),
                COALESCE($5, "_id"($1)));
$$ LANGUAGE SQL;

SELECT 'SELECT * FROM test WHERE' || "where"(null::test, json('{"number" : 1, "_id" : "bar", "_type" : "test"}'));

select "_id"(null::test);
