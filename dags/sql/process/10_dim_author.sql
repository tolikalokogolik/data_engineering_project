DROP TABLE IF EXISTS dim_author CASCADE;
SELECT authors_data_import.author_id,
"Surname",
"Names" INTO dim_author FROM public.authors_data_import;

ALTER TABLE public.dim_author ADD CONSTRAINT dim_author_id_pk PRIMARY KEY (author_id);
COMMIT;
-- COMMIT;
-- INSERT INTO dim_creation_date
-- SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
--        datum AS date_actual,
--        EXTRACT(EPOCH FROM datum) AS epoch,
--        EXTRACT(ISODOW FROM datum) AS day_of_week,
--        EXTRACT(DAY FROM datum) AS day_of_month,
--        TO_CHAR(datum, 'W')::INT AS week_of_month,
--        EXTRACT(WEEK FROM datum) AS week_of_year,
--        EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
--        EXTRACT(MONTH FROM datum) AS month_actual,
--        EXTRACT(YEAR FROM datum) AS year_actual
-- FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
--       FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
--       GROUP BY SEQUENCE.DAY) DQ
-- ORDER BY 1;