# Même base que ton compose
FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
# Crée le répertoire du venv et donne-le à 'airflow'
RUN mkdir -p /opt/airflow/venv && chown -R airflow:0 /opt/airflow/venv


# Empêche toute install --user implicite
ENV PIP_USER=0 PIP_DISABLE_PIP_VERSION_CHECK=1

# Crée le venv et installe dbt + dépendances dans CE venv
RUN python -m venv /opt/airflow/venv \
 && /opt/airflow/venv/bin/python -m pip install --upgrade pip setuptools wheel \
 && /opt/airflow/venv/bin/pip install --no-cache-dir \
      "dbt-core>=1.9,<1.10" "dbt-duckdb>=1.9,<1.10" duckdb==1.3.2 motherduck

# Rends le venv disponible à tous les process
ENV PATH="/opt/airflow/venv/bin:${PATH}"
