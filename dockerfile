FROM apache/airflow:latest
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

-- Additional information on how to build a Airflow Docker Image is described in length in the link below. 
-- https://airflow.apache.org/docs/docker-stack/build.html
