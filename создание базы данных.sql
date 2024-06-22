-- создание базы данных 
CREATE DATABASE chisl_database;
 
-- создание схем
create schema p_chisl_ini;
create schema p_chisl_stg;
create schema p_chisl_core;  

-- установка расширения
CREATE EXTENSION postgres_fdw schema p_chisl_ini;

-- создание внешнего сервера для соединения
CREATE server server_ext
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
   host 'localhost',  -- Хост
   port '5432',       -- Порт
   dbname 'postgres'  -- Имя вашей базы данных
);

-- мэппинг для подключения пользователями
CREATE USER MAPPING FOR CURRENT_USER
SERVER server_ext
OPTIONS (user 'postgres', password '2909');

-- создание внешней таблицы raw_chisl
CREATE FOREIGN TABLE p_chisl_ini.raw_chisl_ext (
	report_date date NULL,
	sh_d int4 NULL,
	oe int4 NULL,
	tn int4 NULL,
	fact_ch numeric NULL,
	shtat_ch numeric NULL,
	vacancies numeric NULL
)
SERVER server_ext
OPTIONS (table_name 'raw_chisl');

-- создание внешней таблицы org_struct_ext
CREATE FOREIGN TABLE p_chisl_ini.org_struct_ext (
	report_date date NULL,
	sh_d int4 NULL,
	sh_d_name varchar(255) NULL,
	oe int4 NULL,
	oe_name varchar(255) NULL,
	te varchar(255) NULL,
	type_te varchar(50) NULL,
	type_podr varchar(50) NULL,
	podr_name varchar(255) NULL,
	typ_detail varchar(255) NULL,
	type_product varchar(255) NULL
)
SERVER server_ext
OPTIONS (table_name 'org_struct');

-- создание внешней таблицы org_struct
eport_date date NULL,
	sh_d int4 NULL,
	sh_d_name varchar(255) NULL,
	oe int4 NULL,
	oe_name varchar(255) NULL,
	te varchar(255) NULL,
	type_te varchar(50) NULL,
	type_podr varchar(50) NULL,
	podr_name varchar(255) -- p_chisl_ini.raw_vacancies_ext определение

-- Drop table

-- DROP FOREIGN TABLE p_chisl_ini.raw_vacancies_ext;

CREATE TABLE p_chisl_ini.raw_vacancies (
	report_date date NULL,
	sh_d int4 NULL,
	tn int4 NULL,
	vac_clear numeric NULL,
	vac_decr numeric NULL,
	vac_time numeric NULL
)

insert into p_chisl_ini.org_struct

-- создаем  podr_name
 CREATE TABLE podr_name  
 (podr_id SERIAL,
   podr_name VARCHAR,
   type_podr VARCHAR,
   te_name VARCHAR,
   product_name VARCHAR,
   PRIMARY KEY (podr_id)
);

CREATE TABLE p_chisl_stg.podr_name (
   podr_id SERIAL,
   podr_name VARCHAR,
   type_podr VARCHAR,
   te_name VARCHAR,
   product_name VARCHAR,
   PRIMARY KEY (podr_id)
);


 -- Удаляем все записи из podr_name
   DELETE FROM podr_name;
   -- Вставляем новые записи из org_struct в podr_name
   INSERT INTO p_chisl_stg.podr_name (podr_name, type_podr, te_name, product_name)
   SELECT
       org.podr_name,
       org.type_podr,
       org.oe_name,
       org.type_product
   FROM
       p_chisl_ini.org_struct org
  group by podr_name, type_podr, oe_name, type_product


-- Создаем таблицу

 sh_d_name
CREATE TABLE p_chisl_stg.sh_d_name (
   sh_d int PRIMARY KEY,
   sh_d_name VARCHAR,
   oe_name VARCHAR
);

-- создание таблицы raw_chisl
CREATE TABLE p_chisl_ini.raw_chisl (
	report_date date,
	sh_d int4,
	oe int4,
	tn int4,
	fact_ch numeric,
	shtat_ch numeric,
	vacancies numeric
)

 -- Удаляем все записи из sh_d_name
   DELETE FROM sh_d_name;
 -- Вставляем новые записи из org_struct в sh_d_name
   INSERT INTO p_chisl_stg.sh_d_name  (sh_d, sh_d_name, oe_name)
   SELECT
       org.sh_d,
       org.sh_d_name,
       org.oe_name
   FROM
       p_chisl_ini.org_struct org
   JOIN (
       SELECT MAX(report_date) AS max_report_date,  sh_d
       FROM  p_chisl_ini.org_struct
       GROUP BY  sh_d
   ) org1 ON org.report_date = org1.max_report_date AND org.sh_d = org1.sh_d
   GROUP BY  org.sh_d,   org.sh_d_name,  org.oe_name


CREATE TABLE p_chisl_stg.vacancy_name (
   vacancy_id SERIAL,
   vacancy_name varchar,
   PRIMARY KEY (vacancy_id)
);

DELETE FROM vacancy_name;
   -- Вставляем новые записи из org_struct в vacancy_name
   INSERT INTO p_chisl_stg.vacancy_name( vacancy_name)
   select
       case when vac_clear > 0 then 'Чистая вакансия'
            when vac_decr > 0 then 'Декретная вакансия'
            when vac_time > 0 then 'Временная вакансия'
       end vacancy_name
   FROM
       p_chisl_ini.raw_vacancies
   group by case when vac_clear > 0 then 'Чистая вакансия'
            when vac_decr > 0 then 'Декретная вакансия'
            when vac_time > 0 then 'Временная вакансия'
       end;

 
– создание таблицы фактов
CREATE TABLE p_chisl_stg.fact_table (
   report_date date,
   sh_d int,
   tab_num int,
   podr_id int,
   shtat_ch numeric,
   fact_ch numeric,
   vacancy numeric,
   vacancy_id int
);      
      
insert into p_chisl_stg.fact_table
SELECT org.report_date,
          org.sh_d,
          ch.tn tab_num,
          p.podr_id,
          ch.shtat_ch,
          ch.fact_ch,
          ch.vacancies,
          case when vac.vac_decr > 0 then 2
               when vac.vac_time > 0 then 3
               when vac.vac_clear > 0 then 1
          end vacancy_id
           from p_chisl_ini.org_struct org
     left join p_chisl_ini.raw_chisl ch
          on org.sh_d = ch.sh_d
          and org.report_date = ch.report_date
  left join p_chisl_stg.podr_name p
          on  org.podr_name=p.podr_name and org.type_podr=p.type_podr
          and org.oe_name = p.te_name and org.type_product  = p.product_name
     left join p_chisl_ini.raw_vacancies vac
          on org.report_date = vac.report_date and org.sh_d = vac.sh_d::int
     group by org.report_date, org.sh_d, ch.tn, podr_id, ch.shtat_ch,  ch.fact_ch, ch.vacancies,
case when vac.vac_decr > 0 then 2
     when vac.vac_time > 0 then 3
     when vac.vac_clear > 0 then 1 end


delete from p_chisl_ini.org_struct where report_date >= '2022-02-01'

select report_date from p_chisl_ini.org_struct group by report_date