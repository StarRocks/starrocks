from pyspark.sql import SparkSession
from datetime import datetime, date

def main():
    # SparkSession.builder.master("local[*]").getOrCreate().stop()
    spark = SparkSession.builder.remote("sc://localhost:10001").getOrCreate()

    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])
    df.show()


if __name__ == "__main__":
    main()
