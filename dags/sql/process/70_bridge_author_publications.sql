DROP TABLE IF EXISTS bridge_author_publications CASCADE;

CREATE TABLE bridge_author_publications
(
    publication_id INT,
    author_id INT
);

INSERT INTO bridge_author_publications(publication_id, author_id) 
SELECT "publication_id", "author_id" FROM public.authors_publications_bridge_import;
ALTER TABLE public.bridge_author_publications ADD CONSTRAINT bridge_author_publications_publication_id_fk FOREIGN KEY (publication_id) REFERENCES fact_publications(publication_id);
ALTER TABLE public.bridge_author_publications ADD CONSTRAINT bridge_author_publications_author_id_fk FOREIGN KEY (author_id) REFERENCES dim_author(author_id);
COMMIT;