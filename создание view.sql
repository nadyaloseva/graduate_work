-- витрина по аналитики вакансий 
CREATE OR REPLACE VIEW p_chisl_core.analyst_vacancies
AS SELECT t2.report_date,
   dp.te_name,
   dp.podr_name,
   dp.product_name,
       CASE
           WHEN sum(t2.fact_ch) = 0 THEN 0
           ELSE sum(t2.kol_uvol_cotr) * 100 / sum(t2.fact_ch)
       END AS turnover_rate,
   avg(t2.vac_lifetime) AS vac_lifetime,
   sum(t2.sum_vacancy) AS sum_vacancy,
   sum(t2.kol_uvol_cotr) AS leaving
  FROM ( SELECT t1.report_date,
           t1.sh_d,
           t1.podr_id,
           sum(t1.kol_uvol_cotr) AS kol_uvol_cotr,
           sum(t1.kol_prin_cotr) AS kol_prin_cotr,
           max(t1.vac_lifetime) AS vac_lifetime,
           sum(t1.fact_ch) AS fact_ch,
           sum(t1.shtat_ch) AS shtat_ch,
           sum(t1.vacancy) AS sum_vacancy
          FROM ( SELECT report_date,
                   sh_d,
                   tab_num,
                   podr_id,
                   fact_ch,
                   shtat_ch,
                   vacancy,
                       CASE
                           WHEN lag(vacancy) OVER (PARTITION BY sh_d, (date_trunc('month', report_date)) ORDER BY report_date) = 0 AND vacancy > 0 THEN 1
                           ELSE 0
                       END AS kol_uvol_cotr,
                       CASE
                           WHEN lag(vacancy) OVER (PARTITION BY sh_d, (date_trunc('month', report_date)) ORDER BY report_date) > 0 AND vacancy = 0 THEN 1
                           ELSE 0
                       END AS kol_prin_cotr,
                       CASE
                           WHEN tab_num = 0  THEN max(report_date) OVER (PARTITION BY sh_d, tab_num ORDER BY report_date) - min(report_date) OVER (PARTITION BY sh_d, tab_num ORDER BY report_date)
                           ELSE NULL
                       END AS vac_lifetime
                  FROM p_chisl_stg.fact_table ch) t1
         GROUP BY t1.report_date, t1.sh_d, t1.podr_id) t2
    LEFT JOIN p_chisl_stg.podr_name dp ON t2.podr_id = dp.podr_id 
 GROUP BY t2.report_date, dp.te_name, dp.podr_name, dp.product_name;

-- витрина по численности
CREATE OR REPLACE VIEW p_chisl_core.analyst_chsil
AS SELECT f.report_date,
   p.te_name,
   p.podr_name,
   p.product_name,
   sum(f.shtat_ch) AS sum_shtat,
   sum(f.fact_ch) AS sum_fact,
   avg(l.lifetime) AS avg_lifetime
  FROM p_chisl_stg.fact_table f
    LEFT JOIN p_chisl_stg.podr_name p ON f.podr_id = p.podr_id
    LEFT JOIN ( SELECT fact_table.tab_num,
           max(fact_table.report_date) - min(fact_table.report_date) AS lifetime
          FROM p_chisl_stg.fact_table
         GROUP BY fact_table.tab_num) l ON f.tab_num = l.tab_num
 GROUP BY f.report_date, p.te_name, p.podr_name, p.product_name;

 -- представление для обновление таблицы фактов 
CREATE OR REPLACE VIEW p_chisl_stg.v_upd_fact_table
as
select REPORT_DATE, SH_D, TAB_NUM, PODR_ID, SHTAT_CH, FACT_CH, VACANCIES, VACANCY_ID
from (SELECT date_trunc('month', org.report_date)::date report_date,
          org.sh_d,
          ch.tn tab_num,
          p.podr_id,
          ch.shtat_ch,
          ch.fact_ch,
          ch.vacancies,
          case when vac.vac_decr > 0 then 'Декретная вакансия'
               when vac.vac_time > 0 then 'Временная вакансия' 
               when vac.vac_clear > 0 then 'Чистая вакансия' 
          end vac_name
           from p_chisl_ini.org_struct org
     left join p_chisl_ini.raw_chisl ch
          on org.sh_d = ch.sh_d
          and org.report_date = ch.report_date
  left join p_chisl_stg.podr_name p
          on  org.podr_name=p.podr_name and org.type_podr=p.type_podr
          and org.oe_name = p.te_name and org.type_product  = p.product_name
     left join p_chisl_ini.raw_vacancies vac
          on org.report_date = vac.report_date and org.sh_d = vac.sh_d::int
     group by date_trunc('month', org.report_date)::date, org.sh_d, ch.tn, podr_id, ch.shtat_ch,  ch.fact_ch, ch.vacancies,
case when vac.vac_decr > 0 then 'Декретная вакансия'
     when vac.vac_time > 0 then 'Временная вакансия' 
     when vac.vac_clear > 0 then 'Чистая вакансия' end) t1
left join p_chisl_stg.vacancy_name v
  on    t1.vac_name = v.vacancy_name
  
  
-- обновление словаря с названием подразделения     
CREATE OR REPLACE VIEW p_chisl_stg.v_upd_podr_name
as SELECT
       org.podr_name,
       org.type_podr,
       org.oe_name,
       org.type_product
   FROM
       p_chisl_ini.org_struct org
  group by podr_name, type_podr, oe_name, type_product
 
-- обновление словаря по название шд   
CREATE OR REPLACE VIEW p_chisl_stg.v_upd_sh_d_name
as  
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
 
-- представление для обновления словаря по названиям вакансий    
CREATE OR REPLACE VIEW p_chisl_stg.v_upd_vacancy_name
as
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
       
call p_chisl_stg.update_table();

SELECT queryid, query, calls, round(total_time) total_time, round(mean_time) mean_time, rows
FROM p_chisl_stg.pg_stat_statements
WHERE query like '%p_chis%'  and query not like '%p_chisl_stg.pg_stat_statements%'-- исключение системных запросов
ORDER BY total_time DESC;

select * from p_chisl_core.analyst_chsil
select * from p_chisl_core.analyst_vacancies
select * from p_chisl_stg.v_upd_fact_table
select * from p_chisl_stg.v_upd_sh_d_name
