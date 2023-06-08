FROM apache/airflow:2.2.5-python3.8

COPY requirements.txt .

RUN pip install -r requirements.txt




