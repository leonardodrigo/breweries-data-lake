FROM apache/airflow:2.10.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow/
WORKDIR /opt/airflow
RUN pip install -r requirements.txt
