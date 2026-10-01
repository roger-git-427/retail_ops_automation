FROM apache/airflow:3.3.2

COPY requirements_ml.txt /requirements_ml.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /requirements_ml.txt
