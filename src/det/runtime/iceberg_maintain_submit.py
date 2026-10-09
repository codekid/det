"""Reference ``DET_ICEBERG_MAINTAIN_SUBMIT`` hook for Iceberg maintain plans.

Logs Spark Iceberg procedure SQL for one plan dict. Does **not** open a Spark
session or run GC.

Wire the reference Airflow DAG::

    DET_ICEBERG_MAINTAIN_SUBMIT=det.runtime.iceberg_maintain_submit:submit_log_spark_sql

Replace this hook with your cluster submitter, for example::

    from det.runtime.iceberg_maintain import render_iceberg_maintain_spark_sql

    def submit(plan: dict) -> None:
        spark = …  # your SparkSession
        for stmt in render_iceberg_maintain_spark_sql(plan):
            spark.sql(stmt)
"""

from __future__ import annotations

from typing import Any

from det.logging import get_logger
from det.runtime.iceberg_maintain import render_iceberg_maintain_spark_sql

logger = get_logger(__name__)


def submit_log_spark_sql(plan: dict[str, Any]) -> None:
    """Render and log Spark maintain SQL for one actionable plan dict.

    Intended as ``DET_ICEBERG_MAINTAIN_SUBMIT`` so mapped DAG tasks exercise the
    submit path without implying physical GC ran.
    """
    stmts = render_iceberg_maintain_spark_sql(plan)
    pipeline = plan.get("pipeline")
    relation = plan.get("sql_relation")
    if not stmts:
        logger.info(
            "iceberg maintain plan produced no SQL",
            pipeline=pipeline,
            sql_relation=relation,
        )
        print(
            f"# iceberg maintain: no SQL for pipeline={pipeline!r} "
            f"relation={relation!r}"
        )
        return
    header = (
        f"# iceberg maintain SQL pipeline={pipeline!r} relation={relation!r} "
        f"statements={len(stmts)} (log-only; not executed)"
    )
    print(header)
    logger.info(
        "iceberg maintain SQL (log-only)",
        pipeline=pipeline,
        sql_relation=relation,
        statement_count=len(stmts),
    )
    for stmt in stmts:
        print(stmt)
        logger.info("iceberg maintain statement", sql=stmt)
