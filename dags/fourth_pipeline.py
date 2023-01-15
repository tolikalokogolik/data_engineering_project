import airflow
import datetime
import py2neo
from py2neo import Graph
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS ={
    'owner':'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'concurrency':0,
    'retries':0,
    'schedule_interval': datetime.timedelta(minutes=1)

}

def clean_db():
    from py2neo import Graph
    g = Graph("bolt://host.docker.internal:7687", user='lol', password='abc')
    query_all = """
    MATCH(m) DETACH DELETE m
    """
    g.run(query_all)



def load_nodes():
    def separate_data_from_publications():


        output_disciplines = '/opt/airflow/dags/disciplines.csv'
        output_publication_types = '/opt/airflow/dags/publication_types.csv'
        output_journals = '/opt/airflow/dags/journals.csv'
        output_relations = '/opt/airflow/dags/relations.csv'
        
        publications = pd.read_csv('/opt/airflow/dags/final_publication_table.csv')
        
        # disciplines
        disciplines = publications.discipline.unique()
        ids = range(len(disciplines))
        disciplines_data = pd.DataFrame({'discipline_id' : ids,
                                    'discipline_name' : disciplines})
        disciplines_data.to_csv(output_disciplines, index=False)

        #publication_types
        publication_types = publications['publication_type'].unique()
        ids = range(len(publication_types))
        publication_types_data = pd.DataFrame({'publication_type_id' : ids,
                                        'publication_type_name' : publication_types})
        publication_types_data.to_csv(output_publication_types, index=False)
        
        journals = publications.journal.unique()
        ids = range(len(journals))
        journals_data = pd.DataFrame({'journal_id' : ids,
                                'journal_name' : journals})
        journals_data.to_csv(output_journals, index=False)
        
        
        #separating relations
        relations = publications.drop(columns=['title', 
                                            'comments', 
                                            'doi', 
                                            'reference_count'])
        relations =relations.rename({'discipline':'discipline_name',
                                     'publication_type':'publication_type_name',
                                     'journal':'journal_name'},
                                     axis=1)

        relations =relations.merge(journals_data, 
                                   on='journal_name',
                                   how='left').merge(publication_types_data,
                                                     on='publication_type_name',
                                                     how='left').merge(disciplines_data,
                                                                       on='discipline_name',
                                                                       how='left')

        relations = relations.drop(columns=['discipline_name',
                                            'publication_type_name',
                                            'journal_name'],
                                axis=1)
        
        relations.to_csv(output_relations, index=False)

    import py2neo
    from py2neo import Graph
    #neo4j/changeme
    # prework
    separate_data_from_publications()
    
    # connection
    g = Graph("bolt://host.docker.internal:7687", user='lol', password='abc')
    
    # queries
    query_authors = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/authors_data.csv' AS row
        CREATE (:author {author_id:row.author_id, 
                         names:row.Names, 
                         surname:row.Surname})
    '''
    query_publications = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/final_publication_table.csv' AS row
        CREATE (:publication {publication_id:row.id, 
                              title:row.title, 
                              comments:row.comments,
                              doi:row.doi,
                              reference_count:row.reference_count})
    '''
    query_publication_types = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/publication_types.csv' AS row
        CREATE (:publication_type {publication_type_id:row.publication_type_id, 
                                   publication_type_name:row.publication_type_name})
    '''
    query_journals = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/journals.csv' AS row
        CREATE (:journal {journal_id:row.journal_id, 
                          journal_name:row.journal_name})
    '''
    query_disciplines = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/disciplines.csv' AS row
        CREATE (:discipline {discipline_id:row.discipline_id, 
                             discipline_name:row.discipline_name})
    '''
    
    #create nodes
    g.run(query_authors)
    g.run(query_publications)
    g.run(query_publication_types)
    g.run(query_journals)
    g.run(query_disciplines)

def load_relationships():
    import py2neo
    from py2neo import Graph
    
    g = Graph("bolt://host.docker.internal:7687", user='neo4j', password='changeme')
    
    query_author_publication ='''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/join_authors_publications.csv' AS row
        MATCH (p:publication {publication_id:row.publication_id}),
              (a:author {author_id:row.author_id})
        CREATE (a)-[:CREATE]->(p)
    '''
    
    query_publication_discipline = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/relations.csv' AS row
        MATCH (p:publication {publication_id:row.id}),
              (d:discipline {discipline_id:row.discipline_id})
        CREATE (p)-[:PART_OF]->(d)
    '''
    
    query_publication_publication_type = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/relations.csv' AS row
        MATCH (p:publication {publication_id:row.id}),
              (pt:publication_type {publication_type_id:row.publication_type_id})
        CREATE (p)-[:PART_OF]->(pt)
    '''
    
    query_publication_journal = '''
        LOAD CSV WITH HEADERS FROM 'file:///opt/airflow/dags/relations.csv' AS row
        MATCH (p:publication {publication_id:row.id}),
              (j:journal {journal_id:row.journal_id})
        CREATE (p)-[:SENT_TO]->(j)
        CREATE (j)-[:PUBLISH]->(p)
    '''
    g.run(query_author_publication)
    g.run(query_publication_discipline)
    g.run(query_publication_publication_type)
    g.run(query_publication_journal)

with DAG(
    dag_id='neo4j_dag',
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:

    clean_task = PythonOperator(
        task_id = 'clean_task',
        python_callable = clean_db
    )

    nodes_task = PythonOperator(
        task_id = 'nodes_task',
        python_callable = load_nodes
    )

    relationships_task = PythonOperator(
        task_id = 'relationships_task',
        python_callable = load_relationships
    )

clean_task >> nodes_task >> relationships_task