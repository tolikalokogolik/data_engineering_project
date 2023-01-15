DROP TABLE IF EXISTS fact_publications CASCADE;

SELECT 
    pi."id" AS publication_id,
    pi."title",
    pi."comments",
    pi."doi",
    pi."reference_count",
    ds.discipline_id,
    dpt.publication_type_id,
    dj.journal_id
    INTO fact_publications
FROM public.publications_import pi
LEFT JOIN dim_discipline ds ON pi.discipline = ds.discipline_name
LEFT JOIN dim_publication_type dpt ON pi.publication_type = dpt.publication_type_name
LEFT JOIN dim_journal dj ON pi.journal = dj.journal_name;

ALTER TABLE IF EXISTS public.fact_publications
    ADD CONSTRAINT act_publications_discipline_id_fk FOREIGN KEY (discipline_id) REFERENCES dim_discipline(discipline_id) MATCH SIMPLE
    ON UPDATE NO ACTION 
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.fact_publications
    ADD CONSTRAINT fact_publications_journal_id_fk FOREIGN KEY (journal_id) REFERENCES dim_journal(journal_id) MATCH SIMPLE
    ON UPDATE NO ACTION 
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.fact_publications
    ADD CONSTRAINT fact_publications_publication_type_id_fk FOREIGN KEY (publication_type_id) REFERENCES dim_publication_type(publication_type_id) MATCH SIMPLE
    ON UPDATE NO ACTION 
    ON DELETE NO ACTION
    NOT VALID;

-- SELECT DISTINCT ON (publication_id) *
-- FROM fact_publications
DELETE FROM fact_publications
WHERE (publication_id, title, comments, doi, reference_count, discipline_id, publication_type_id, journal_id) NOT IN (
    SELECT DISTINCT ON (publication_id) publication_id, title, comments, doi, reference_count, discipline_id, publication_type_id, journal_id
    FROM fact_publications
);


ALTER TABLE fact_publications
ADD CONSTRAINT fact_publications_id_pk PRIMARY KEY (publication_id);
--ALTER TABLE public.fact_publications ADD CONSTRAINT fact_publications_publication_type_id_fk FOREIGN KEY (publication_type_id) REFERENCES dim_publication_type(publication_type_id);

-- --CREATE TABLE fact_publications
-- (
--     publication_id INT,
--     title VARCHAR(500),
--     comments VARCHAR(500),
--     doi VARCHAR(500),
--     reference_count INT,
--     discipline_id INT,
--     publication_type_id INT,
--     journal_id INT
-- );
-- INSERT INTO fact_publications (publication_id)
-- SELECT 
--     "id"
-- FROM public.publications_import;
-- INSERT INTO fact_publications (title, comments, doi, reference_count, discipline_id, publication_type_id, journal_id)
-- SELECT 
--     pi."title",
--     pi."comments",
--     pi."doi",
--     pi."reference_count",
--     ds.discipline_id,
--     dpt.publication_type_id,
--     dj.journal_id
-- FROM public.publications_import pi
-- LEFT JOIN dim_discipline ds ON pi.discipline = ds.discipline_name
-- LEFT JOIN dim_publication_type dpt ON pi.publication_type = dpt.publication_type_name
-- LEFT JOIN dim_journal dj ON pi.journal = dj.journal_name;

--ALTER TABLE public.fact_publications ADD CONSTRAINT fact_publications_id_pk PRIMARY KEY (publication_id);

--ALTER TABLE public.fact_publications ADD CONSTRAINT fact_publications_discipline_id_fk FOREIGN KEY (discipline_id) REFERENCES dim_discipline(discipline_id);

COMMIT;