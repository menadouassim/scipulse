"""
SciPulse — DAG principal : pipeline ArXiv (batch)

download -> MinIO -> ingestion -> réindexation (mapping optimisé) -> validation GX ->
nettoyage -> miroir PostgreSQL -> dbt -> enrichissement -> validation GX finale
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from src.utils.pipeline_log import airflow_callback

RUN = "cd /opt/airflow && python -m"

default_args = {
    "owner": "scipulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": airflow_callback("success"),
    "on_failure_callback": airflow_callback("failed"),
}

with DAG(
    dag_id="scipulse_arxiv_pipeline",
    default_args=default_args,
    description="Pipeline batch ArXiv : ingestion, transformation, enrichissement",
    schedule=None,  # déclenchement manuel
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["scipulse", "arxiv", "batch"],
) as dag:

    # 1. Dump ArXiv : fichier déjà présent, sinon Kaggle (si credentials), sinon échantillon synthétique
    download_arxiv = BashOperator(
        task_id="download_arxiv_dump",
        bash_command="""
            set -e
            DIR=/opt/airflow/data/arxiv-raw
            mkdir -p "$DIR"
            if ls "$DIR"/*.json >/dev/null 2>&1; then
                echo "✅ Dump déjà présent :"; ls -lh "$DIR"
            elif [ -n "$KAGGLE_USERNAME" ] && [ -n "$KAGGLE_KEY" ]; then
                kaggle datasets download -d Cornell-University/arxiv -p "$DIR" --unzip
                cd /opt/airflow && python -m src.ingestion.prepare_dump "$DIR"/arxiv-metadata-oai-snapshot.json "$DIR"/arxiv-cs-subset.json
            else
                echo "⚠️  Pas de dump ni de credentials Kaggle — génération d'un échantillon synthétique"
                cd /opt/airflow && python -m src.ingestion.generate_sample --n 2000 --out "$DIR"/arxiv-sample.json
            fi
        """,
    )

    # 2. Landing zone MinIO
    upload_minio = BashOperator(
        task_id="upload_to_minio",
        bash_command=f"""
            set -e
            F=$(ls /opt/airflow/data/arxiv-raw/*.json | head -1)
            {RUN} src.ingestion.upload_to_minio "$F"
        """,
    )

    # 3. Ingestion (même résultat que le pipeline Logstash : _id = arxiv_id, idempotent)
    ingest_raw = BashOperator(
        task_id="ingest_arxiv_raw",
        bash_command=f"{RUN} src.ingestion.arxiv_bulk_ingest --minio",
    )

    # 4. Mapping optimisé + réindexation
    reindex = BashOperator(
        task_id="reindex_arxiv_papers",
        bash_command=f"{RUN} src.ingestion.reindex_arxiv --recreate",
    )

    # 5. Validation des données brutes (Great Expectations)
    validate_raw = BashOperator(
        task_id="validate_raw_arxiv",
        bash_command=f"{RUN} src.quality.validate_arxiv",
    )

    # 6. Nettoyage guidé par la validation
    clean = BashOperator(
        task_id="clean_arxiv",
        bash_command=f"{RUN} src.quality.clean_arxiv",
    )

    # 7. Miroir PostgreSQL + dbt
    load_pg = BashOperator(
        task_id="load_postgres_mirror",
        bash_command=f"{RUN} src.transform.load_postgres",
    )
    transform_dbt = BashOperator(
        task_id="transform_dbt",
        bash_command="""
            set -e
            cd /opt/airflow/dbt
            ARGS="--profiles-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
            dbt seed $ARGS && dbt run $ARGS && dbt test $ARGS
        """,
    )

    # 8. Enrichissement NLP + croisement HN
    enrich = BashOperator(
        task_id="enrich_arxiv_python",
        bash_command=f"{RUN} src.enrichment.enrich_arxiv",
    )

    # 9. Validation finale
    validate_enriched = BashOperator(
        task_id="validate_enriched",
        bash_command=f"{RUN} src.quality.validate_arxiv --enriched",
    )

    (
        download_arxiv >> upload_minio >> ingest_raw >> reindex >> validate_raw >> clean
        >> load_pg >> transform_dbt >> enrich >> validate_enriched
    )
