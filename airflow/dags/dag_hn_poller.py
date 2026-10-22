"""
SciPulse — DAG secondaire : poller Hacker News (micro-batch)
Exécuté toutes les 5 minutes pour récupérer les derniers items HN.
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
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_success_callback": airflow_callback("success"),
    "on_failure_callback": airflow_callback("failed"),
}

with DAG(
    dag_id="scipulse_hn_poller",
    default_args=default_args,
    description="Polling Hacker News : ingestion micro-batch + détection liens ArXiv",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scipulse", "hacker-news", "streaming"],
) as dag:

    poll_hn = BashOperator(
        task_id="poll_hn_api",
        bash_command=f"{RUN} src.ingestion.hn_poller",
    )

    detect_links = BashOperator(
        task_id="detect_arxiv_links",
        bash_command=f"{RUN} src.enrichment.detect_arxiv_links",
    )

    update_scores = BashOperator(
        task_id="update_arxiv_hn_scores",
        bash_command=f"{RUN} src.enrichment.update_hn_scores",
    )

    poll_hn >> detect_links >> update_scores
