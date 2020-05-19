from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import sys


def find_id(house, street, street_dict):
    import re
    import bisect

    street = street.upper()

    house = re.sub("[^0-9]", "", house)
    if house != "":
        if street not in street_dict:
            return None
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

    counts = {}

    boros = [0, dict(), dict(), dict(), dict(), dict()]
    boro_id = [1, 2, 3, 4, 5]

    for single_id in boro_id:
        with open(SparkFiles.get("boro_" + str(single_id) + ".csv")) as f:
            for line in f.readlines():
                street_name = line.split(",")[0]
                rest = ','.join(line.split(",")[1:])
                boros[single_id][street_name] = literal_eval(rest)
                boros[single_id][street_name][1] += [None]
                boros[single_id][street_name][3] += [None]
            f.close()

    if pid == 0:
        next(records)

    reader = csv.reader(records)

    for row in reader:
        county = row[21]
        num = row[23]
        st = row[24]

        issue_year = int(row[4].split("/")[-1]) - 2015

        if issue_year in [0, 1, 2, 3, 4]:
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
                    if zoneid not in counts:
                        counts[zoneid] = [0, 0, 0, 0, 0]
                    counts[zoneid][issue_year] += 1
                    # counts[zoneid] = counts.get(zoneid, 0) + 1
    return counts.items()


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    # path = "hdfs:///data/share/bdm/nyc_cscl.csv"
    # path = "hdfs:///user/nliu/boros/boro_1.csv"
    sc.addFile("hdfs:///user/nliu/boros/boro_1.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_2.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_3.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_4.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_5.csv")

    # df = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv", header=True, multiLine=True, escape='"')

    # rdd = df.select(df['Violation County'], df['House Number'], df['Street Name']).rdd

    rdd = sc.textFile('/data/share/bdm/nyc_parking_violation/2015.csv')

    counts = rdd.mapPartitionsWithIndex(process).reduceByKey(lambda x, y: x + y)

    counts.show()

    print(counts.collect())
