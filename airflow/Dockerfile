# Local DET + Airflow image (dev only). Build context is the repo root.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install DET so DAG tasks can `import det`. The compose mount overlays /opt/det
# with the host checkout for live configs/fixtures/code.
COPY --chown=airflow:0 pyproject.toml README.md /opt/det/
COPY --chown=airflow:0 src /opt/det/src

WORKDIR /opt/det
# No Airflow constraints file: it pins duckdb==1.1.x while DET needs >=1.5.
RUN pip install --no-cache-dir -e ".[dbt]"
