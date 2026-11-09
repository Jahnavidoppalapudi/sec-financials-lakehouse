FROM apache/airflow:3.3.2-python3.12

# dbt gets its own virtual environment so its dependencies never conflict with Airflow's.
# The venv is created without pip and filled using the image's own pip.
USER root
COPY requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv --without-pip /opt/dbt-venv \
 && PIP_USER=false python -m pip --python /opt/dbt-venv/bin/python install --no-cache-dir \
      -r /requirements-dbt.txt \
 && mkdir -p /opt/airflow/warehouse \
 && chown 50000:0 /opt/airflow/warehouse
USER airflow

COPY requirements.txt /requirements.txt
# Pin Airflow itself so extra packages can never upgrade or downgrade it
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
