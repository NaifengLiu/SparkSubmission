import bisect

records = [1,5,4,2,6]

# records = list(enumerate(records))
#
# print(records)
#
# [(key, record)] = sorted(records, key=lambda x:x[1])

ranges = [1,3,5,7]

for each in records:
    partitionId = bisect.bisect_left(ranges, each)

    print(partitionId)

# print(a)











