FROM apache/airflow:3.0.6-python3.11

COPY requirements.txt /tmp/requirements.txt
# Pin apache-airflow in the same resolve so extra packages cannot upgrade it.
RUN pip install --no-cache-dir "apache-airflow==3.0.6" -r /tmp/requirements.txt
