---
Title: "Data Engineering Project"
Author: "Natali Atamanova, Musa Salamov, Valerija Jerina"
Content: "Data Engineering (LTAT.02.007) Group Project, January 2023"
Date: 
---

# Data Engineering Project Group 1

Group Project for Data Engineering (LTAT.02.007), reporting in January 2023.
Group members: Natali Atamanova, Musa Salamov,Valerija Jerina

Our Git Repository can be found [HERE](https://github.com/tolikalokogolik/data_engineering_project)

Data Source in from [Kaggle](https://www.kaggle.com/datasets/Cornell- University/arxiv?resource=download)


## Setup

use ```Dockerfile``` and ```docker-compose.yaml``` for creating image.

## Running the pipeline
In order to run Airflow services:
```sh
docker-compose up
```
If you want to run Airflow services to run in the background, then add `-d`.

Once everything has started, it is possible to log in to the web interface and try running tasks.
* The Airflow web server is available at: `http://localhost:8080`. 
* The default account for Airflow has login `airflow` and password `airflow`
* The Neo4J Browser UI is available at: `http://localhost:7474`

### Execution Order

Please make sure to execute the DAGs (each dag represents a pipeline) in the following order, as they partially rely on files created in the previous DAG:
1. Filtering DAG
2. Enrichment and Augmentation DAG
3. Postgres DAG
4. Neo4J DAG

We intentionally want to trigger the DAGs more or less manually as there is no actual need for scheduling and this also gives more control
about the timing, which is especially handy for testing. 
  

## Cleaning up

Docker Desktop can be helpful to remove the containers and re-start them.

If you want to stop and remove containers, networks, volumes and images created by `docker-compose up` run:
```sh
docker-compose down --volumes --remove-orphans
```

---

## Pipeline Design Decisions

Following part describes pipelines that are used in this project:
1) Ingestion and Filtering
2) Enrichment & Augmentation
3) DWH
4) Graph View

![Pipeline design](images/Blank diagram.PNG) 

---

### Pipeline 1 - Ingestion & Wrangling

In the first pipeline we get the original data, filter it and store it in a suitable format. Source is https://owncloud.ut.ee/owncloud/s/4rTHn9Ex9KXQwkM/download/publications20k.json.

Input: 
- ```publications20k.json```

Output:
- ```publications.json```

Tasks:
- ```download_json```: downloads [source file](https://owncloud.ut.ee/owncloud/s/4rTHn9Ex9KXQwkM/download/publications20k.json)
- ```clean_the_data```: There are several intermediary tasks are done here: Dropping the columns which are not necessary such as ["abstract", "license"] and etc. The publication entries with less than 35 characters in their title is dropped from dataset. IDs are resetted.
- ```transform_data```: Data has versions column which includes all the versions of a publication, and here following new columns are made: when the publication initially is submitted/created, and what is the latest update date.

Intermediary tasks:

1. ```download_task```
2. ```clean_the_data```
3. ```transform_data```

First pipeline view in Airflow:
![Thirdpipeline](images/pipeline1.png) 

---

### Pipeline 2 - Enrichment

In the second pipeline, the output dataset of the first pipeline is enriched by requesting additional information from CrossRefAPI. The following two tasks are creating reference count and authors datasets accordingly. According to the names of authors, genders of the authors are determined via Gender-API. It also includes the tasks such as creating a seperate publication versions and different join table csv files.

Input:
- ```publications.json```

Output: 
- ```publications.json```
- ```final_publication_table.csv```
- ```gender_data.csv```
- ```authors_data.csv```
- ```join_authors_genders.csv```
- ```join_authors_publications.csv```
- ```publ_versions_data.csv``` 
- ```reference_count_data.csv``` 
- ```sql/reference_count_data_import.sql```
- ```sql/publications_import.sql```
- ```sql/genders_import.sql```
- ```sql/authors_data_import.sql```
- ```sql/publ-versions-schema.sql```



Tasks:
- ```enrich_with_crossrefapi```
  - Using crossrefapi library for requesting the reference count, discipline, publication type and journal.
  - Updating the current dataset with requested items, by adding them as columns into the dataset.
- ```create_reference_count_data```
  - Taking publication id and corresponding reference count into a data frame, and creating sql schema accordingly.
  - Output: reference_count_data.csv and reference_count_data_import.sql
- ```create_authors_data```
  - Creating a dataframe containing the author id, surname and name of the authors.
  - Stores it into authors_data.csv and create sql schema stored in authors_data_import.sql
- ```create_gender_data```
  - Taking the first names of Submitter for each publication, and request a gender information from Gender-API.
  - Gathered information regarding genders for the names is stored into gender_data.csv
  - Because of limitations of the API, only 100 requests are available per month. Therefore, once it is requested and gender_data file is stored, it doesn't request the information from API again.
- ```create_publ_versions_data```
  - As a publication can have several versions, the separate dataframe is created to store versions of publications.
  - It stores the version into publ_versions_data.csv and create corresponding sql schema named publ_versions_data_import.sql
- ```create_join_tables```
  - Creating join tables for authors and gender information, authors and publications.
  - It also creates the necessary sql schemas such as publications_import.sql, authors_publications_bridge_import.sql, genders_import.sql
  - Here also the final version of full publications dataset is defined, dropping unnecassary columns. It is stored into final_publication_table.csv;

Second pipeline view in Airflow:
![Thirdpipeline](images/pipeline2.png) 

---

### Pipeline 3 - Data Warehouse
In the third pipeline, we use SQL schemas and different csv files that have been generated in the previous pipelines. So far, population files were written manually. We import our csv tables into PostgreSQL database. Connection to the postgres has been stup in `docker-compose.yaml` :  
``` 
x-airflow-common:
  environment:
    AIRFLOW_CONN_POSTGRES_DEFAULT: 'postgres://airflow:airflow@postgres:5432'
```
Since later we will need to give Postgres an opportunity to access csv files, we have changed output directory for Airflow DAG, so the `.yaml` filed contained the following:
 ```
services:
  postgres:
    volumes:
      - ./dags:/import
```
However, those connections won't be seen for the normal user since they have been set up through env.variables.
We understand that this solution is not suitable for the real production, but it simplified the process.
In order to create most of out tables, we have generated scripts in previous pipeline, then we have population triggers trat populate every single table. To do this, `PostgresOperator` have been used, but `-population.sql` files allowed it to be executed by the system. In general, those files(in the folder `/dags/sql/`) allowed us to simplify process alot so far.

Moreover, proposed snowflake schema has been changed in order for the project to be more functional(at some pont). The publications table has been changed to the fact table, instead of fact table we implemented bridge table(to connect publications and authors), then used author names to store data about the gender. One of the main reasons for the last change has been caused by *GENDER API* that gave only 100 downloads per month. That made us think of a way to use it. Another thing that has been removed is affiliation. Most of the data that *crossref API* has returned contained well... almost nothing. Thus, we decided that it would be a storage waste to add this(yet we did notice that some names for some unknown reason contained affiliation name in it). Also, we have dropped some values from the original publication table not to store junk which will tell us nothing. 
Third pipeline view in Airflow:
![Thirdpipeline](images/pipeline3.png) 

Fact Table in our Schema(fact_publications) mainly focuses on using the data obtained from the second pipeline and to make it easier for BI queries to work.
Dimension Tables include:
- *dim_author* - a list of authors with assigned id's
- *dim_gender* - table that uses authors_id as fk and has corresponding gender in it. 
- *dim_discipline* - table that contains information on unique discipline names and their id's
- *dim_journal* - table that contains information on unique journals names and their id's
- *dim_publication_type* - table that contains information on qnique publication types names and their id's
Also includes:
- Bridge table - table that connects author id to publication id's
- Publication version table - includes publication version as FK, and includes different dates as the versions.


Sample DW queries:

![Thirdpipeline1](images/sql_q1.png) 

![Thirdpipeline2](images/sql_q2.png)

![Thirdpipeline3](images/sql_q3.png)

![Thirdpipeline4](images/sql_q4.png) 

---

### Pipeline 4 - Graph View

The last pipeline builds a graph view of publications, authors, disciplines, jornals, publications types and their relationships using Neo4J. 
The nodes are the publications, authors, disciplines, jornals and the edges represent CREATE, PART_OF, SENT_TO or PUBLISH  relationships between nodes (see Figure 2 for schema). 
The original dataset have only CREATE relation between author(s) and publication. We enriched the original dataset with data from API and got new relationships PART_OF, SENT_TO and PUBLISH.


The data is ingested into graph database from a csv file that is the output of the first and second pipelines. 
final_publication_table data is partioned to journals, disciplines, publications types and relations tables. 
Relations table storage relations between publications and separated data. 

![Figure 2](images/neo4j_schema.png)

The graph view allows addressing the following query questions:
1) Discovery of co-authors ~ find out the authors who co-authored with same person, so the
   person can introduce those authors to work on the relative discipline to contribute
2) Discovery of top n authors by citations counts of their publications.
3) Discovery of top n journals by publications references count.

The results of the above queries are presented below:
1) Co-authors discover of E. L. Berge:

![co-authors](images/1query.PNG)

2) TOP 5 authors with their citations count:

![top 5](images/2query.PNG)

3) TOP 5 journals with their citations count:

![top 5_2](images/3query.PNG)

Fourth pipeline view in Airflow:

![Thirdpipeline](images/pipeline4.png) 

----

## Contribution

Since it has been a group project, every single member contributed into the project more or less equally. This project had daily discussions, every single member has been helping each other and answering the questions that appeared during the process.
However, the main responsible persons for the pipelines were:

- Pipeline 1: Natali, Valerija, Musa 
- Pipeline 2: Musa
- Pipeline 3: Valerija
- Pipeline 4: Natali

