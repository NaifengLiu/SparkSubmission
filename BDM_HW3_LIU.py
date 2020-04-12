from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()

    complaints_path = sys.argv[1]
    output_path = sys.argv[2]

    cp = sc.textFile(complaints_path, use_unicode=True).cache()


    def extractScores(partId, records):
        if partId == 0:
            next(records)
        import csv
        reader = csv.reader(records)
        for row in reader:
            year = row[0].split('-')[0]
            product = row[1]
            company = row[7]
            yield ((product, year), company)


    cp_detail = cp.mapPartitionsWithIndex(extractScores)

    d = cp_detail.map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0][0], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: (x[0], x[1], int(x[1] / x[0] * 100)))

    lines = d.map(lambda x: ','.join(x[0])+','+','.join([str(integer) for integer in x[1]]))
    lines.saveAsTextFile(output_path)


