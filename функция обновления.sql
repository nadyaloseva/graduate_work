-- функция обновления
CREATE PROCEDURE p_chisl_stg.update_table() 
LANGUAGE SQL
AS $$
  TRUNCATE TABLE p_chisl_stg.podr_name;
  INSERT INTO p_chisl_stg.podr_name (podr_name, type_podr, te_name, product_name)
  SELECT org.podr_name,
    org.type_podr,
    org.oe_name,
    org.type_product
   FROM p_chisl_ini.org_struct org
  GROUP BY org.podr_name, org.type_podr, org.oe_name, org.type_product;
 
  TRUNCATE TABLE p_chisl_stg.sh_d_name;
  INSERT INTO p_chisl_stg.sh_d_name
  SELECT * FROM p_chisl_stg.v_upd_sh_d_name;
 
  TRUNCATE TABLE p_chisl_stg.vacancy_name;
  INSERT INTO p_chisl_stg.vacancy_name(vacancy_name)
  SELECT
        CASE
            WHEN raw_vacancies.vac_clear > 0 THEN 'Чистая вакансия'
            WHEN raw_vacancies.vac_decr > 0 THEN 'Декретная вакансия'
            WHEN raw_vacancies.vac_time > 0 THEN 'Временная вакансия'
            ELSE NULL
        END AS vacancy_name
   FROM p_chisl_ini.raw_vacancies
  GROUP BY (
        CASE
            WHEN raw_vacancies.vac_clear > 0 THEN 'Чистая вакансия'
            WHEN raw_vacancies.vac_decr > 0 THEN 'Декретная вакансия'
            WHEN raw_vacancies.vac_time > 0 THEN 'Временная вакансия'
            ELSE NULL
        END);

  TRUNCATE TABLE p_chisl_stg.fact_table;
  INSERT INTO p_chisl_stg.fact_table
  SELECT * FROM p_chisl_stg.v_upd_fact_table;
$$;


CREATE OR REPLACE FUNCTION p_chisl_stg.grant_privileges_on_schemas(
    role_name TEXT,
    schema_list TEXT[]
)
RETURNS VOID AS $$
DECLARE
    r RECORD;
    schema_name TEXT;
BEGIN
    FOREACH schema_name IN ARRAY schema_list
    LOOP
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = schema_name) LOOP
            EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' || schema_name || '.' || r.tablename || ' TO ' || role_name;
        END LOOP;

        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ' || schema_name || 
                ' GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ' || role_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE ROLE data_engeener; -- дата инженер
SELECT grant_privileges_on_schemas(
    data_engeener, 
    ARRAY[‘p_chisl_ini’, ‘p_chisl_stg’, ‘p_chisl_core’] );
   
   CREATE OR REPLACE FUNCTION p_chisl_stg.grant_privileges_on_schemas(
    role_name TEXT,
    schema_list TEXT[]
)
RETURNS VOID AS $$
DECLARE
    r RECORD;
    schema_name TEXT;
BEGIN
    FOREACH schema_name IN ARRAY schema_list
    LOOP
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = schema_name) LOOP
            EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' || schema_name || '.' || r.tablename || ' TO ' || role_name;
        END LOOP;

        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ' || schema_name || 
                ' GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ' || role_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE ROLE data_engeener; -- дата инженер
SELECT p_chisl_stg.grant_privileges_on_schemas(
    'data_engeener'::TEXT, 
    ARRAY['p_chisl_ini'::TEXT, 'p_chisl_stg'::TEXT, 'p_chisl_core'::TEXT]::TEXT[]
);


SELECT p_chisl_stg.grant_privileges_on_schemas(
    'support_group'::TEXT, 
    ARRAY[ 'p_chisl_ini'::TEXT] );
   
 CREATE ROLE analyst;  
SELECT p_chisl_stg.grant_privileges_on_schemas(
    'analyst'::TEXT, 
    ARRAY[ 'p_chisl_core'::TEXT] );

SELECT 
    grantee, 
    table_catalog, 
    table_schema, 
    string_agg(privilege_type, ', ') AS privilege_type
FROM 
    information_schema.table_privileges
WHERE 
    grantee NOT LIKE 'postgres' and  grantee NOT LIKE 'PUBLIC'
    AND table_schema like 'p_chisl%'
GROUP BY 
    grantee, 
    table_catalog, 
    table_schema;
   
 drop   
   
 select grantee from information_schema.table_privileges group by grantee
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA p_chisl_core TO data_engeener;

