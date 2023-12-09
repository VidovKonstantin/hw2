import pytest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from soda.scan import Scan


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()


def build_scan(name, spark_session):
    scan = Scan()
    scan.disable_telemetry()
    scan.set_scan_definition_name("data_quality_test")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark_session)
    return scan

def test_comments(spark):
    comments_schema = StructType([ \
    StructField("comm_video_id", StringType(), True), \
    StructField("comment_text", StringType(), True), \
    StructField("comment_likes", IntegerType(), True), \
    StructField("comment_replies", IntegerType(), True)])
    comments = (spark.read
                .option('header', 'true')
                .schema(comments_schema)
                .option("mode", "DROPMALFORMED")
                .option("columnNameOfCorruptRecord", "corrupt_record")
                .csv(r".\datasets\UScomments.csv")
                )
    comments.createOrReplaceTempView('sdf_comments')
    scan = build_scan("comments_data_quality_test_1", spark)
    scan.add_sodacl_yaml_file(r"data_quality/comments_checks2.yaml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()