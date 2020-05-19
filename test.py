from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkFiles
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

    with open(SparkFiles.get("nyc_cscl.csv")) as csv_file:
        tmp = csv.DictReader(csv_file, delimiter=',')
        for item in tmp:
            # print(item)
            if item['FULL_STREE'] not in streets_list.keys():
                streets_list[item['FULL_STREE']] = []
            streets_list[item['FULL_STREE']].append(
                [item['PHYSICALID'], item['FULL_STREE'], item['ST_NAME'], item['L_LOW_HN'], item['L_HIGH_HN'],
                 item['R_LOW_HN'], item['R_HIGH_HN']])

    print("!!!!!!!!!!!!!!")
    print("!!!!!!!!!!!!!!")
    print("!!!!!!!!!!!!!!")
    print("!!!!!!!!!!!!!!")

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
    sqlContext = SQLContext(sc)

    # path = "hdfs:///data/share/bdm/nyc_cscl.csv"
    # sc.addFile(path)

    df_violation = spark.read.csv("/data/share/bdm/nyc_parking_violation/2015.csv", header=True, multiLine=True, escape='"')
    rdd_violation = df_violation.select(df_violation['Violation County'].alias('County'),
                                        df_violation['House Number'].cast('int').alias('HouseNumber'),
                                        df_violation['Street Name'].alias('StreetName'))

    # counts = rdd_violation.mapPartitions(process).reduceByKey(lambda x, y: x + y).collect()

    df_clcs = spark.read.csv("/data/share/bdm/nyc_cscl.csv", header=True, multiLine=True, escape='"')
    rdd_clcs = df_clcs.select(df_clcs['PHYSICALID'],
                              df_clcs['FULL_STREE'],
                              df_clcs['ST_NAME'],
                              df_clcs['L_LOW_HN'].cast('int'),
                              df_clcs['L_HIGH_HN'].cast('int'),
                              df_clcs['R_LOW_HN'].cast('int'),
                              df_clcs['R_HIGH_HN'].cast('int'),

                              )

    # print(rdd_violation.collect(15))
    # print(rdd_clcs.collect(15))

    rdd_violation.registerTempTable("v")
    rdd_clcs.registerTempTable("c")

    results = sqlContext.sql("SELECT c.PHYSICALID FROM v INNER JOIN c ON "
                             "(v.StreetName==c.FULL_STREE or v.StreetName=c.ST_NAME)"
                             "and ("
                             "(v.HouseNumber%2==1 and v.HouseNumber<=c.L_HIGH_HN and v.HouseNumber>=c.L_LOW_HN)"
                             "or"
                             "(v.HouseNumber%2==0 and v.HouseNumber<=c.R_HIGH_HN and v.HouseNumber>=c.R_LOW_HN)"
                             ")")

    # print(results.collect())
    results.collect().saveAsTextFile("2222")



    # joined_df = df_violation.join(df_clcs).filter(dates_df.lower_timestamp < events_df.time).filter(
    #     events_df.time < dates_df.upper_timestamp)
