SELECT dim_discipline.discipline_name,
(SUM(CASE WHEN dim_gender.gender = 'male' THEN 1 ELSE 0 END) / COUNT(DISTINCT bridge_author_publications.author_id)::float) AS proportion_male,
(SUM(CASE WHEN dim_gender.gender = 'female' THEN 1 ELSE 0 END) / COUNT(DISTINCT bridge_author_publications.author_id)::float) AS proportion_female
FROM dim_discipline
JOIN fact_publications ON fact_publications.discipline_id = dim_discipline.discipline_id
JOIN bridge_author_publications ON bridge_author_publications.publication_id = fact_publications.publication_id
JOIN dim_author ON dim_author.author_id = bridge_author_publications.author_id
JOIN dim_gender ON dim_gender.author_id = dim_author.author_id
GROUP BY dim_discipline.discipline_name;