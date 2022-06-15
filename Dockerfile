FROM apache/airflow:2.3.0
USER airflow
COPY ./requirements.txt /opt/app/requirements.txt
RUN pip install -r /opt/app/requirements.txt
