DROP TABLE IF EXISTS dim_discipline CASCADE;

CREATE TABLE dim_discipline
(
    discipline_id INT,
    discipline_name VARCHAR(100)
);
ALTER TABLE public.dim_discipline ADD CONSTRAINT dim_discipline_id_pk PRIMARY KEY (discipline_id);

INSERT INTO dim_discipline(discipline_id, discipline_name) 
SELECT DISTINCT ROW_NUMBER() OVER (ORDER BY publication_type) AS discipline_id, "discipline" FROM public.publications_import;

COMMIT;