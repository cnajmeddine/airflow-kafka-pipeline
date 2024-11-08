FROM apache/airflow:2.3.3

# Install required Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-postgres

# Copy Airflow DAG file
COPY dags/ /opt/airflow/dags/

# Switch to the airflow user
USER airflow