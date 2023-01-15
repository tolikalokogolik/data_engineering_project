DROP TABLE IF EXISTS dim_gender CASCADE;
SELECT genders_import.author_id,
"gender" INTO dim_gender FROM public.genders_import;

ALTER TABLE public.dim_gender ADD CONSTRAINT dim_gender_author_id_fk FOREIGN KEY (author_id) REFERENCES dim_author(author_id) ;
COMMIT;