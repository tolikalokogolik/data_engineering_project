WITH hindex AS (
    SELECT
        author_id,
        RANK() OVER (ORDER BY SUM(reference_count) DESC) as hindex
    FROM
        fact_publications
        JOIN bridge_author_publications ON fact_publications.publication_id = bridge_author_publications.publication_id
    GROUP BY
        author_id
)

SELECT
    dim_author.author_id,
    dim_author."Names",
    dim_author."Surname",
    hindex
FROM
    hindex
    JOIN dim_author ON hindex.author_id = dim_author.author_id
ORDER BY
    hindex DESC
LIMIT 5;