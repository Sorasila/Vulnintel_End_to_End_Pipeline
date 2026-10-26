def test_spark_session_runs_local(spark):
    assert spark.range(3).count() == 3
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
