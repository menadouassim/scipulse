from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


def test_dags_import_without_errors():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    assert {"scipulse_arxiv_pipeline", "scipulse_hn_poller"} <= set(bag.dag_ids)


def test_arxiv_dag_order():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    dag = bag.get_dag("scipulse_arxiv_pipeline")
    order = [t.task_id for t in dag.topological_sort()]
    assert order.index("validate_raw_arxiv") < order.index("transform_dbt") < order.index("validate_enriched")
