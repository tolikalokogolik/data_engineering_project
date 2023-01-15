DROP TABLE IF EXISTS dim_publication_type CASCADE;

CREATE TABLE dim_publication_type
(
    publication_type_id INT,
    publication_type_name VARCHAR(100)
);
ALTER TABLE public.dim_publication_type ADD CONSTRAINT dim_publication_type_id_pk PRIMARY KEY (publication_type_id);

INSERT INTO dim_publication_type(publication_type_id, publication_type_name) 
SELECT DISTINCT ROW_NUMBER() OVER (ORDER BY publication_type) AS publication_type_id, "publication_type" FROM public.publications_import;

COMMIT;