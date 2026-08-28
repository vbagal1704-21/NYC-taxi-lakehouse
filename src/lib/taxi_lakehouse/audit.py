"""Pipeline run auditing.

Every task writes a start row and an end row (status, metrics) to
<catalog>.ops.pipeline_runs. This is the operational backbone every
enterprise pipeline has: you can answer "what ran, when, how many rows,
did it fail and why" with one query.
"""

import json
import time
import uuid
from datetime import datetime, timezone


class AuditLogger:
    def __init__(self, spark, cfg, task_name: str, run_id: str | None = None):
        self.spark = spark
        self.cfg = cfg
        self.task_name = task_name
        # One logical run id shared by all tasks of a job run when the job
        # passes {{job.run_id}}; otherwise generate one.
        self.run_id = run_id or str(uuid.uuid4())
        self._start = None

    def start(self, extra: dict | None = None):
        self._start = time.time()
        self._write("RUNNING", metrics=extra or {})
        return self

    def success(self, metrics: dict | None = None):
        self._write("SUCCESS", metrics=metrics or {})

    def failure(self, error: str, metrics: dict | None = None):
        self._write("FAILED", metrics=metrics or {}, error=error[:4000])

    def _write(self, status: str, metrics: dict, error: str | None = None):
        elapsed = round(time.time() - self._start, 2) if self._start else None
        row = [
            (
                self.run_id,
                self.task_name,
                self.cfg.env,
                self.cfg.run_month,
                status,
                json.dumps(metrics),
                error,
                elapsed,
                datetime.now(timezone.utc),
            )
        ]
        schema = (
            "run_id string, task_name string, env string, run_month string, "
            "status string, metrics string, error string, "
            "elapsed_seconds double, logged_at timestamp"
        )
        (
            self.spark.createDataFrame(row, schema)
            .write.mode("append")
            .saveAsTable(self.cfg.ops_pipeline_runs)
        )


def audited(spark, cfg, task_name: str, run_id: str | None = None):
    """Context-manager flavour:

        with audited(spark, cfg, "silver_transform") as audit:
            ... do work ...
            audit.metrics["rows_written"] = n
    """

    class _Ctx:
        def __init__(self):
            self.logger = AuditLogger(spark, cfg, task_name, run_id)
            self.metrics: dict = {}

        def __enter__(self):
            self.logger.start()
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.logger.success(self.metrics)
            else:
                self.logger.failure(f"{exc_type.__name__}: {exc}", self.metrics)
            return False  # re-raise

    return _Ctx()
