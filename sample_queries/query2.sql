SELECT dim_journal.journal_name, COUNT(fact_publications.publication_id) as total_publications
FROM dim_journal
JOIN fact_publications ON dim_journal.journal_id = fact_publications.journal_id
GROUP BY dim_journal.journal_name ORDER BY total_publications DESC LIMIT 3;