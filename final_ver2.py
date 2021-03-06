from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkFiles
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from statistics import mean
import numpy as np
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


def calculate_OLS_coeff(y):
    y_mean = mean(y)
    if sum(y) != 0:
        return ((y[0] - y_mean) * (-2) + (y[1] - y_mean) * (-1) + (y[3] - y_mean) + (y[4] - y_mean) * 2) / 10
    else:
        return 0


def process(pid, records):
    import csv
    from ast import literal_eval

    counts = {}

    boros = [0, dict(), dict(), dict(), dict(), dict()]
    boro_id = [1, 2, 3, 4, 5]

    all = []

    with open(SparkFiles.get("all_cscl.csv")) as f:
        for line in f.readlines():
            all.append(line.rstrip())

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

        issue_year = row[4].split("/")[-1]

        if str(issue_year) in ['2015', '2016', '2017', '2018', '2019']:
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
    for item in all:
        if item not in counts:
            counts[item] = [0, 0, 0, 0, 0]
    return counts.items()


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    outpath = sys.argv[1]

    # path = "hdfs:///data/share/bdm/nyc_cscl.csv"
    # path = "hdfs:///user/nliu/boros/boro_1.csv"
    sc.addFile("hdfs:///user/nliu/boros/boro_1.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_2.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_3.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_4.csv")
    sc.addFile("hdfs:///user/nliu/boros/boro_5.csv")
    sc.addFile("hdfs:///user/nliu/boros/all_cscl.csv")

    # df = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv", header=True, multiLine=True, escape='"')

    # rdd = df.select(df['Violation County'], df['House Number'], df['Street Name']).rdd

    rdd = sc.textFile('/data/share/bdm/nyc_parking_violation')

    import operator

    counts = rdd.mapPartitionsWithIndex(process) \
        .reduceByKey(lambda x, y: list(map(operator.add, x, y))).map(
        lambda x: [x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], str(calculate_OLS_coeff(x[1]))])
    # counts = rdd.mapPartitionsWithIndex(process) \
    #     .reduceByKey(lambda x, y: list(map(operator.add, x, y)))

    results = sqlContext.createDataFrame(counts,
                                         ["PHYSICALID", "COUNT_2015", "COUNT_2016", "COUNT_2017", "COUNT_2018",
                                          "COUNT_2019", "OLS_COEF"])

    # results = results.orderBy('PHYSICALID').rdd
    results = results.orderBy('PHYSICALID')

    results.show(100)

    results.write.csv(outpath)

    # results = results.map(
    #     lambda x: str(x[0]) + ',' + ','.join([str(integer) for integer in x[1]]) + ',' + str(calculate_OLS_coeff(x[1])))

    # print(counts.collect())

    # counts.saveAsTextFile(outpath)

    # counts.map(lambda x:)

    # df = sqlContext.createDataFrame(counts, ["PHYSICALID", "count"])
    #
    # results = df.orderBy('PHYSICALID').agg(calculate_OLS_coeff(df['count']).alias("OLS"))
    #
    # results.show(1000)
    #
    # results = results.rdd
    #
    # # print(results.collect())
    #
    # from ast import literal_eval
    #
    # r = results.map(lambda x: str(x[0]) + "," + ",".join(
    #     [str(integer) for integer in literal_eval(str(x[1]))]) + "," + calculate_OLS_coeff(literal_eval(str(x[1]))))
    #
    # # df_final = sqlContext.createDataFrame(counts, ["PHYSICALID", "count"])
    #
    # # r = results.map(lambda x: str(x[0]) + "," + ",".join(
    # #     [str(integer) for integer in literal_eval(str(x[1]))]) + "," + calculate_OLS_coeff(literal_eval(str(x[1]))) if x[1] is not None else str(x[0])+",0,0,0,0,0,0")
    #
    # r.saveAsTextFile(outpath)
