FROM apache/airflow:2.2.2
RUN pip install --upgrade pip
RUN	pip install ijson
RUN pip install py2neo
RUN	pip install alt-profanity-check
ARG SSL_KEYSTORE_PASSWORD
USER root
RUN apt-get update && apt-get install -y jq