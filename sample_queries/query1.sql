SELECT dim_discipline.discipline_name, COUNT(fact_publications.publication_id) as total_publications
FROM dim_discipline 
JOIN fact_publications ON dim_discipline.discipline_id = fact_publications.discipline_id
GROUP BY dim_discipline.discipline_name ORDER BY total_publications DESC LIMIT 5;