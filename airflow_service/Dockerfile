FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pyarrow==18.0.0 \
    sqlalchemy==2.0.46 \
    psycopg2-binary==3.3.2 \
    psycopg==3.3.2 \
    requests==2.32.5 \
    pandas==3.0.0 \
    tqdm==4.67.3
