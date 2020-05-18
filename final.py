from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    df = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv",
                        header=True, multiLine=True, escape='"')

    df = df.select(df['Violation County'], df['House Number'], df['Street Name'])
    df.write.csv('2015')

    df = spark.read.csv("/data/share/bdm/nyc_cscl.csv",
                        header=True, multiLine=True, escape='"')
    df = df.select(df['PHYSICALID'], df['FULL_STREET'], df['ST_NAME'], df['L_LOW_HN'], df['L_HIGH_HN'], df['R_LOW_HN'],
                   df['R_HIGH_HN'])
    df.write.csv('cscl')
