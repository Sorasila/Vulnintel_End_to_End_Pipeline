import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local single-threaded SparkSession shared by all Spark tests.

    These settings are for local tests only and are not part of production code.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("vulnintel-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
