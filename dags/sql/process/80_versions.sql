DROP TABLE IF EXISTS dim_versions CASCADE;
SELECT "id" AS publication_id,
  "version" INTO dim_versions FROM public.publ_versions_data_import;

ALTER TABLE IF EXISTS public.dim_versions
    ADD CONSTRAINT dim_versions_publication_id_fk FOREIGN KEY (publication_id) REFERENCES fact_publications(publication_id) MATCH SIMPLE
    ON UPDATE NO ACTION 
    ON DELETE NO ACTION
    NOT VALID;
COMMIT;