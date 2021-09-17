FROM apache/airflow:2.1.2

USER root
RUN apt-get update -y
RUN apt-get install -y --no-install-recommends build-essential git
USER airflow
RUN pip install dbt==0.20.0 && \
    pip install git+https://github.com/Tomme/dbt-athena.git && \
    pip install airflow-dbt==0.4.0
