import airflow
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS ={
    'owner':'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'concurrency':0,
    'retries':0,
    'schedule_interval': datetime.timedelta(minutes=1)

}

with DAG(
    dag_id="postgres_dag",
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:
    create_gender_data_import = PostgresOperator(
        task_id="create_gender_data_import",
        postgres_conn_id="postgres_default",
        sql="sql/genders_import.sql",
    )
    populate_gender_data_import = PostgresOperator(
        task_id="populate_gender_data_import",
        postgres_conn_id="postgres_default",
        sql="sql/genders_import-population.sql",
    )

    create_publication_versions = PostgresOperator(
        task_id="create_publication_versions",
        postgres_conn_id="postgres_default",
        sql="sql/publ-versions-schema.sql",
    )
    populate_publication_versions = PostgresOperator(
        task_id="populate_publication_versions",
        postgres_conn_id="postgres_default",
        sql="sql/publ-versions-population.sql",
    )
    
    create_authors_data_import = PostgresOperator(
        task_id="create_authors_data_import",
        postgres_conn_id="postgres_default",
        sql="sql/authors_data_import.sql",
    )
    populate_authors_data_import = PostgresOperator(
        task_id="populate_authors_data_import",
        postgres_conn_id="postgres_default",
        sql="sql/authors_data_import-population.sql",
    )
    create_dim_author = PostgresOperator(
        task_id="create_dim_author",
        postgres_conn_id="postgres_default",
        sql="sql/process/10_dim_author.sql",
    )

    create_publications_import = PostgresOperator(
        task_id="create_publications_import",
        postgres_conn_id="postgres_default",
        sql="sql/publications_import.sql",
    )
    populate_publications_import = PostgresOperator(
        task_id="populate_publications_import",
        postgres_conn_id="postgres_default",
        sql="sql/publications_import-population.sql",
    )

    create_dim_gender = PostgresOperator(
        task_id="create_dim_gender",
        postgres_conn_id="postgres_default",
        sql="sql/process/20_dim_gender.sql",
    )
    create_dim_journal = PostgresOperator(
        task_id="create_dim_journal",
        postgres_conn_id="postgres_default",
        sql="sql/process/30_dim_journal.sql",
    )
    create_dim_discipline = PostgresOperator(
        task_id="create_dim_discipline",
        postgres_conn_id="postgres_default",
        sql="sql/process/40_dim_discipline.sql",
    )
    create_dim_publication_type = PostgresOperator(
        task_id="create_dim_publication_type",
        postgres_conn_id="postgres_default",
        sql="sql/process/50_dim_publication_type.sql",
    )

    create_fact_publications = PostgresOperator(
        task_id="create_fact_publications",
        postgres_conn_id="postgres_default",
        sql="sql/process/60_fact_publications.sql",
    )

    create_authors_publications_bridge_import = PostgresOperator(
        task_id="create_authors_publications_bridge_import",
        postgres_conn_id="postgres_default",
        sql="sql/authors_publications_bridge_import.sql",
    )

    populate_authors_publications_bridge_import= PostgresOperator(
        task_id="populate_authors_publications_bridge_import",
        postgres_conn_id="postgres_default",
        sql="sql/authors_publications_bridge_import_population.sql",
    )
    create_bridge_authors_publications = PostgresOperator(
        task_id="create_bridge_authors_publications",
        postgres_conn_id="postgres_default",
        sql="sql/process/70_bridge_author_publications.sql",
    )

    create_bridge_versions = PostgresOperator(
        task_id="create_bridge_versions",
        postgres_conn_id="postgres_default",
        sql="sql/process/80_versions.sql",
    )
   
#airflow tasks initiation
    [create_authors_publications_bridge_import >> populate_authors_publications_bridge_import,
    create_publication_versions >> populate_publication_versions,
    [create_authors_data_import >> populate_authors_data_import >> create_dim_author,
    create_gender_data_import >> populate_gender_data_import  ] >> create_dim_gender,
    create_publications_import >> populate_publications_import >> [create_dim_journal,create_dim_discipline,
    create_dim_publication_type] >> create_fact_publications]   >> create_bridge_authors_publications >> create_bridge_versions
 
