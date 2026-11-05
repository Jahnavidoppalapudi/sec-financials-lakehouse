FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
# Pin Airflow itself so extra packages can never upgrade or downgrade it
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
