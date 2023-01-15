DROP TABLE IF EXISTS dim_journal CASCADE;

CREATE TABLE dim_journal
(
    journal_id INT,
    journal_name VARCHAR(50) NOT NULL
);
ALTER TABLE public.dim_journal ADD CONSTRAINT dim_journal_id_pk PRIMARY KEY (journal_id);

INSERT INTO dim_journal(journal_id, journal_name) 
SELECT DISTINCT ROW_NUMBER() OVER (ORDER BY journal) AS journal_id, "journal" FROM public.publications_import;

--INSERT INTO dim_journal(journal_name) VALUES (SELECT ROW_NUMBER() OVER () As journal_id FROM dim_journal);
--SELECT DISTINCT "journal" INTO dim_journal FROM public.publications_import;
--INSERT INTO dim_journal(journal_name) VALUES (SELECT DISTINCT "journal" FROM public.publications_import);

--SELECT ROW_NUMBER() OVER () As journal_id INTO dim_journal FROM dim_journal;
--ALTER TABLE dim_journal ADD journal_id INT;
--CREATE UNIQUE INDEX journal_id ON dim_journal;
--CREATE UNIQUE INDEX journal_id ON dim_journal (journal_id INT);


--ALTER TABLE public.dim_gender ADD CONSTRAINT dim_gender_author_id_fk FOREIGN KEY (author_id) REFERENCES dim_author(author_id);
COMMIT;
