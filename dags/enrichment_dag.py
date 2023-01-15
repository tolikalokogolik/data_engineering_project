import requests
import math
import json
import datetime
import airflow
import random 
import pandas as pd
import numpy as np
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.fs_hook import FSHook

vision_entries = "/opt/airflow/dags/vision_entries.json"
vision = "/opt/airflow/dags/vision.json"

DEFAULT_ARGS ={
    'owner':'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'concurrency':0,
    'retries':0,
    'schedule_interval': datetime.timedelta(minutes=1)

}
second_dag = DAG(
    dag_id='enrichment-and-augmentation', # name of dag

    default_args=DEFAULT_ARGS, # args assigned to all operators
)

def enrich_with_crossref():
    filename = "/opt/airflow/dags/publications.json"
    output_filename = "/opt/airflow/dags/publications.json"
    # output_filename_reference_data = "/opt/airflow/dags/reference_count_data.csv"
    # output_unique_disciplines = "/opt/airflow/dags/unique_disciplines.csv"
    # output_unique_publication_types = "/opt/airflow/dags/unique_publication_types.csv"
    # output_unique_journals = "/opt/airflow/dags/unique_journals.csv"

    schema_output_filename = "/opt/airflow/dags/sql/reference_count_schema.sql"

    data = pd.read_json(filename, orient="index")


    from crossref.restful import Works

    def request_crossrefapi(row):
        works = Works()
        try:
            response = [works.doi(row).get(k) for k in ['is-referenced-by-count', 'subject', 'type', 'publisher']]
        except:
            response = None
            
        return response

    # def is_referenced_by(row):
    #     works = Works()
    #     try:
    #         referenced_by_count = works.doi(row)["is-referenced-by-count"]
    #     except:
    #         referenced_by_count = None
            
    #     return referenced_by_count
    
    #data_split = np.array_split(data, 200)

    # for i in range(200):
    #     data_split[i]['is_referenced_by'] = data_split[i]["doi"].apply(is_referenced_by)

    # data = pd.concat(data_split)
    data = data.replace(to_replace='None', value=np.nan)
    data = data.dropna(subset=['doi'])

    data = data.head(100)

    #data['is_referenced_by'] = data["doi"].apply(is_referenced_by)

    data["crossrefresponse"]= data["doi"].apply(request_crossrefapi)
    data[['reference-count','discipline', 'publication-type', 'journal']] = pd.DataFrame(data.crossrefresponse.tolist(), index= data.index)
    
    # data['discipline'] = data['discipline'].apply(lambda x: x[0] if x else None)
    # reference_count_data = data[['id', "reference-count"]].copy()

    # reference_count_data.to_csv(output_filename_reference_data, index=False)
    
    # def create_distinct_values(column_name):
    #     data_discipline = data[[f'column_name']].copy()
        
    #     return data_discipline.drop_duplicates()

    # unique_disciplines = create_distinct_values("discipline")
    # unique_publication_types = create_distinct_values("publication-type")
    # unique_journals = create_distinct_values("journal")

    # unique_disciplines.to_csv(output_unique_disciplines, index=False)
    # unique_publication_types.to_csv(output_unique_publication_types, index=False)
    # unique_journals.to_csv(output_unique_journals, index=False)

    data = data.drop(columns=["crossrefresponse"])
    data.to_json(output_filename, orient="index")

first_task = PythonOperator(
    task_id='enrich_with_crossref',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=enrich_with_crossref,
)


def create_refence_count_data():
    filename = "/opt/airflow/dags/publications.json"
    output_filename_reference_data = "/opt/airflow/dags/reference_count_data.csv"

    schema_output_reference_count_data = "/opt/airflow/dags/sql/reference_count_data_import.sql"


    data = pd.read_json(filename, orient="index")


 
    reference_count_data = pd.DataFrame(data[['id', "reference-count"]].copy())

    reference_count_data.to_csv(output_filename_reference_data, index=False)

    with open(schema_output_reference_count_data, 'w') as f:
        f.write('DROP TABLE IF EXISTS reference_count_data_import;\n')
        f.write(pd.io.sql.get_schema(reference_count_data, 'reference_count_data_import'))
    
    # def create_distinct_values(column_name):
    #     new_data = data[column_name].copy()

    #     unique_values = new_data.drop_duplicates()
        
    #     return unique_values

    # unique_disciplines = pd.DataFrame(create_distinct_values("discipline"))
    # unique_publication_types = pd.DataFrame(create_distinct_values("publication-type"))
    # unique_journals = pd.DataFrame(create_distinct_values("journal"))



    


second_task = PythonOperator(
    task_id='create_refence_count_data',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=create_refence_count_data,
)

first_task >> second_task

def create_authors_data():
    filename = "/opt/airflow/dags/publications.json"
    output_filename = "/opt/airflow/dags/publications.json"
    output_filename_authors_data = "/opt/airflow/dags/authors_data.csv"
    schema_output_authors_data = "/opt/airflow/dags/sql/authors_data_import.sql"
    data = pd.read_json(filename, orient="index")

    def authors_data():
        surnames = []
        names = [] 
        for i in data["authors_parsed"]:
            for k in range(len(i)):
                surnames.append(i[k][0])
                names.append(' '.join(i[k][1:]))

        fullnames = {'Surname':surnames, 'Names':names}
        return pd.DataFrame(fullnames).drop_duplicates()
    
    data.to_json(output_filename, orient="index")
    authors_data = authors_data()
    authors_data = authors_data.reset_index().rename(columns={'index':'author_id'})

    authors_data = pd.DataFrame(authors_data)
    authors_data.to_csv(output_filename_authors_data, index=False)

    with open(schema_output_authors_data, 'w') as f:
        f.write('DROP TABLE IF EXISTS authors_data_import;\n')
        f.write(pd.io.sql.get_schema(authors_data, 'authors_data_import'))
    

third_task = PythonOperator(
    task_id='create_authors_data',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=create_authors_data,
)

second_task >> third_task

def create_gender_data():
    filename = "/opt/airflow/dags/publications.json"
    # output_filename = "/opt/airflow/dags/publications.json"
    output_filename_gender_data = "/opt/airflow/dags/gender_data.csv"
    # schema_output_gender_data = "/opt/airflow/dags/sql/gender_data_import.sql"

    data = pd.read_json(filename, orient="index")

    import os
    if os.path.exists(output_filename_gender_data):
        gender_data = pd.read_csv(output_filename_gender_data)
        pass
    else:

        def gender(row):
            import json
            from urllib.request import urlopen
            myKey = "cjdATNeZPdoTFAK4vj4qaRHhv3npCTdjyKbU"
            try:
                url = "https://gender-api.com/get?key=" + myKey + f"&name={row.firstname}"
                response = urlopen(url)
                decoded = response.read().decode('utf-8')
                gender_data = json.loads(decoded)

                gender = gender_data["gender"]
            except:
                gender = None

            return gender

        def get_name(row):
            return row.submitter.split()[0]

        gender_data = pd.DataFrame({'firstname' : data.apply(get_name, axis=1).unique()})
        gender_data = gender_data[~gender_data["firstname"].str.contains('\.' )]
        gender_data['gender'] = gender_data.apply(gender, axis=1)



        gender_data.to_csv(output_filename_gender_data, index=False)

    # with open(schema_output_gender_data, 'w') as f:
    #     f.write('DROP TABLE IF EXISTS gender_data_import;\n')
    #     f.write(pd.io.sql.get_schema(gender_data, 'gender_data_import'))


fourth_task = PythonOperator(
    task_id='create_gender_data',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=create_gender_data,
)

third_task >> fourth_task

def create_publ_versions_data():
    filename = "/opt/airflow/dags/publications.json"
    # output_filename = "/opt/airflow/dags/publications.json"
    output_filename_publ_versions_data = "/opt/airflow/dags/publ_versions_data.csv"
    schema_output_filename = "/opt/airflow/dags/sql/publ-versions-schema.sql"

    data = pd.read_json(filename, orient="index")

    new_rows = []

    # Iterate over the rows of the initial dataframe
    for index, row in data.iterrows():
        # Iterate over the versions in the current row
        for version in row['versions']:
            # Append a new row to the list with the current ID and version
            new_rows.append({'id': row['id'], 'version': datetime.datetime.strptime(version['created'], "%a, %d %b %Y %H:%M:%S %Z")})

    publ_versions_data = pd.DataFrame(new_rows, columns=['id', 'version'])
    publ_versions_data.to_csv(output_filename_publ_versions_data, index=False)

    with open(schema_output_filename, 'w') as f:
        f.write('DROP TABLE IF EXISTS publ_versions_data_import;\n')
        f.write(pd.io.sql.get_schema(publ_versions_data, 'publ_versions_data_import'))

fifth_task = PythonOperator(
    task_id='create_publ_versions_data',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=create_publ_versions_data,
)

fourth_task >> fifth_task



######################################################
def create_join_tables():

    filename = "/opt/airflow/dags/publications.json"
    authors_data_filename = "/opt/airflow/dags/authors_data.csv"
    gender_data_filename = "/opt/airflow/dags/gender_data.csv"
    output_join_authors_genders= "/opt/airflow/dags/join_authors_genders.csv"
    output_join_authors_publications = "/opt/airflow/dags/join_authors_publications.csv"
    final_publications_filename = "/opt/airflow/dags/final_publication_table.csv"
    schema_output_final_data = "/opt/airflow/dags/sql/publications_import.sql"

    schema_output_authors_publications_bridge = "/opt/airflow/dags/sql/authors_publications_bridge_import.sql"
    schema_output_genders = "/opt/airflow/dags/sql/genders_import.sql"

    authors_data = pd.read_csv(authors_data_filename)
    gender_data = pd.read_csv(gender_data_filename)

    data = pd.read_json(filename, orient="index")

    author_ids = []
    publication_ids = []
    author_ids2 = []
    genders = []


    for i in range(data.shape[0]):

        # Data for genders_authors_join table
        sub_name = data["submitter"].iloc[i].split()[0]
        sub_surname = data["submitter"].iloc[i].split()[-1]

    # skip problematic names
        try:
            authors_subdata = authors_data[(authors_data["Names"].str.contains(sub_name[0])) & (authors_data["Surname"].str.contains(sub_surname))]
            gender_subdata = gender_data[gender_data["firstname"] == sub_name]
            if (gender_subdata.gender.iloc[0] != 'unknown'):
                author_ids.append(authors_subdata.author_id.iloc[0])
                genders.append(gender_subdata.gender.iloc[0])
        except:
            print('Problematic name: ' + data["submitter"].iloc[i])



        for j in range(len(data["authors_parsed"].iloc[i])):
            # Data for publications_authors_join table
            surname = data["authors_parsed"].iloc[i][j][0]
            names = ' '.join(data["authors_parsed"].iloc[i][j][1:])
            authors_subdata = authors_data[(authors_data["Names"].str.contains(names)) & (authors_data["Surname"].str.contains(surname))]
            if authors_subdata.shape[0] > 0:
                author_ids2.append(authors_subdata.author_id.iloc[0])
                publication_ids.append(data.id.iloc[i])

    join_authors_publications = pd.DataFrame({'publication_id' : publication_ids,
                                            'author_id' : author_ids2})

    join_authors_genders = pd.DataFrame({'author_id' : author_ids,
                                        'gender' : genders})

    join_authors_publications.to_csv(output_join_authors_publications, index=False)
    join_authors_genders.to_csv(output_join_authors_genders, index=False)
    necessarry_columns = ["id", "title", "comments", "doi", "reference-count", "discipline", "publication-type", "journal"]

    final_data = pd.DataFrame(data[necessarry_columns])
    final_data.rename(columns = {'reference-count':'reference_count', 'publication-type':'publication_type'}, inplace = True)
    final_data['discipline'] = final_data['discipline'].apply(lambda x: x[0] if x else None)


    # def create_distinct_values(column_name):
    #     new_data = data[column_name].copy()

    #     unique_values = new_data.drop_duplicates()
        
    #     return unique_values

    # unique_disciplines = pd.DataFrame(create_distinct_values("discipline"))
    # unique_publication_types = pd.DataFrame(create_distinct_values("publication-type"))
    # unique_journals = pd.DataFrame(create_distinct_values("journal"))


    final_data.to_csv(final_publications_filename, index=False)

    with open(schema_output_final_data, 'w') as f:
        f.write('DROP TABLE IF EXISTS publications_import;\n')
        f.write(pd.io.sql.get_schema(final_data, 'publications_import'))

    with open(schema_output_authors_publications_bridge, 'w') as f:
        f.write('DROP TABLE IF EXISTS authors_publications_bridge_import;\n')
        f.write(pd.io.sql.get_schema(join_authors_publications, 'authors_publications_bridge_import'))

    with open(schema_output_genders, 'w') as f:
        f.write('DROP TABLE IF EXISTS genders_import;\n')
        f.write(pd.io.sql.get_schema(join_authors_genders, 'genders_import'))

    

sixth_task = PythonOperator(
    task_id='create_join_tables',
    dag=second_dag,
    trigger_rule='none_failed',
    python_callable=create_join_tables,
)
fifth_task >> sixth_task