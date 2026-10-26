"""Spike S3: pipeline `spike_s3_b` (catalog vulnintel_dev, schema ops) มี materialized view เดียว 1000 แถว."""

from pyspark import pipelines as dp


@dp.materialized_view
def spike_s3_b_mv():
    return spark.range(1000)  # noqa: F821 (spark ถูก inject โดย pipeline runtime)
