import pytest

from video_analytics.functions import split_tags
from chispa import *
import pyspark.sql.functions as F


def test_split_tags(spark):
    data = [
        ("jo|se", "jo"),
        ("**|**", "**"),
        ("#:|:lui", "#:"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
             .withColumn("clean_name", split_tags(F.col("name")).getItem(0))
                    )
    assert_column_equality(df, "clean_name", "expected_name")


def test_split_tags_nice_error(spark):
    data = [
        ("jo|se", "jo"),
        ("**|**", "**"),
        ("#:|:lui", "#:"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])\
                .withColumn("clean_name", split_tags(F.col("name")).getItem(0))
                )
    assert_column_equality(df, "clean_name", "expected_name")