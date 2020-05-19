from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys


def procecss_num(string):
    import re
    string = re.sub("[^0-9]", "", string)
    if string == '':
        return 0
    else:
        return int(string)


def find_id(house, street, list_of_streets):
    import re
    house = re.sub("[^0-9]", "", house)
    if str(house).isnumeric():
        house = int(house)
        tmp = list_of_streets[street]
        for item in tmp:
            if house % 2 == 1:
                if house >= procecss_num(item[3]) & house <= procecss_num(item[4]):
                    return item[0]
            else:
                if house >= procecss_num(item[5]) & house <= procecss_num(item[6]):
                    return item[0]
    return None


def process(records):
    import csv

    reader = csv.reader(records)

    counts = {}

    streets_list = dict()

    with open('/data/share/bdm/nyc_cscl.csv') as csv_file:
        tmp = csv.DictReader(csv_file, delimiter=',')
        for item in tmp:
            print(item)
            if item['FULL_STREE'] not in streets_list.keys():
                streets_list[item['FULL_STREE']] = []
            streets_list[item['FULL_STREE']].append(
                [item['PHYSICALID'], item['FULL_STREE'], item['ST_NAME'], item['L_LOW_HN'], item['L_HIGH_HN'],
                 item['R_LOW_HN'], item['R_HIGH_HN']])

    for row in reader:
        county = row[0]
        num = row[1]
        st = row[2]
        zoneid = find_id(num, st, streets_list)
        if zoneid:
            counts[zoneid] = counts.get(zoneid, 0) + 1
    return counts.items()


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    # complaints_path = "2015.csv"

    # rdd = sc.textFile(complaints_path, use_unicode=True).cache()

    # # cscl = sc.textFile("cscl.csv", use_unicode=True).cache()
    # df = spark.read.csv("cscl.csv",
    #                     header=True, multiLine=True, escape='"')
    #
    # # df = df.filter(df.st1 == '8th Ave')
    #
    # # d = find_id(158, '8th Ave', df)

    df = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv",
                                            header=True, multiLine=True, escape='"')

    rdd = df.select(df['Violation County'], df['House Number'], df['Street Name']).rdd

    counts = rdd.mapPartitions(process).reduceByKey(lambda x, y: x + y).collect()

    counts.saveAsTextFile("2222")
