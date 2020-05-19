from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import sys


def find_id(house, street, street_dict):
    import re
    import bisect
    house = re.sub("[^0-9]", "", house)
    if house != "":
        options = street_dict[street]
        house = int(house)
        if house % 2 == 1:
            return options[1][bisect.bisect_left(options[0], house)]
        else:
            return options[3][bisect.bisect_left(options[2], house)]
    return None


def process(pid, records):
    import csv
    from ast import literal_eval

    reader = csv.reader(records)

    counts = {}

    streets_list = dict()

    boros = [0, dict(), dict(), dict(), dict(), dict()]
    boro_id = [1,2,3,4,5]

    for single_id in boro_id:
        with open("boro_" + str(single_id) + ".csv") as f:
            for line in f.readlines():
                street_name = line.split(",")[0]
                rest = ','.join(line.split(",")[1:])
                boros[single_id][street_name] = literal_eval(rest)
            f.close()

    if pid==0:
        next(records)
    for row in reader:
        county = row[0]
        num = row[1]
        st = row[2]

        nyc_boro_mapping = dict()
        nyc_boro_mapping['NY'] = 1
        nyc_boro_mapping['BX'] = 2
        nyc_boro_mapping['BK'] = 3
        nyc_boro_mapping['K'] = 3
        nyc_boro_mapping['Q'] = 4
        nyc_boro_mapping['QN'] = 4
        nyc_boro_mapping['ST'] = 5

        if county in nyc_boro_mapping:
            zoneid = find_id(num, st, boros[nyc_boro_mapping[county]])
            if zoneid:
                counts[zoneid] = counts.get(zoneid, 0) + 1
    return counts.items()


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    # path = "hdfs:///data/share/bdm/nyc_cscl.csv"
    # path = "hdfs:///user/nliu/boros/boro_1.csv"
    # sc.addFile("hdfs:///user/nliu/boros/boro_1.csv")
    # sc.addFile("hdfs:///user/nliu/boros/boro_2.csv")
    # sc.addFile("hdfs:///user/nliu/boros/boro_3.csv")
    # sc.addFile("hdfs:///user/nliu/boros/boro_4.csv")
    # sc.addFile("hdfs:///user/nliu/boros/boro_5.csv")

    df = spark.read.csv("2015.csv", header=True, multiLine=True, escape='"')

    rdd = df.select(df['Violation County'], df['House Number'], df['Street Name']).rdd

    print(rdd.collect())

    counts = rdd.mapPartitionsWithIndex(process).reduceByKey(lambda x, y: x + y).collect()



    counts.saveAsTextFile("2222")
