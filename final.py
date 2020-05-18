from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    df = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv",
                        header=True, multiLine=True, escape='"')

    df = df.select(df['Violation County'], df['House Number'], df['Street Name'])
    d = df.collect(5).saveAsTextFile('1')




























