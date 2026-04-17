"""Verify the logistics_pipeline DAG loads without errors."""
from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow", reason="Airflow not installed")

from airflow.models import DagBag  # noqa: E402


def test_dag_loads_without_errors() -> None:
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "logistics_pipeline" in dag_bag.dags
    assert dag_bag.import_errors == {}


def test_dag_has_expected_task_count() -> None:
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.dags["logistics_pipeline"]
    assert len(dag.tasks) == 8


def test_dag_task_dependencies() -> None:
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.dags["logistics_pipeline"]

    tasks = {t.task_id: t for t in dag.tasks}

    assert tasks["extract_weather_task"].upstream_task_ids == {"extract_orders_task"}
    assert tasks["transform_silver_orders_task"].upstream_task_ids == {"extract_orders_task"}
    assert tasks["transform_silver_diesel_task"].upstream_task_ids == {"extract_diesel_task"}
    assert tasks["transform_silver_weather_task"].upstream_task_ids == {"extract_weather_task"}
    assert tasks["build_gold_task"].upstream_task_ids == {
        "transform_silver_orders_task",
        "transform_silver_diesel_task",
        "transform_silver_weather_task",
    }
    assert tasks["load_gold_task"].upstream_task_ids == {"build_gold_task"}
