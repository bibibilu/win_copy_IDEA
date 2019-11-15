from typing import List

from pyspark import SparkContext
import sys, time, random


def comlist(record):
    record[1][0].append(record[1][1])
    return record[0], record[1][0]


def xx(x):
    print(x)

# def hashFunction(x):
#     ran = [71, 277, 691, 1699, 2087, 4211, 6329, 9043, 12613]
#     a = random.randint(2, 100)
#     b = random.randint(2, 100)
#     p = ran[random.randrange(0, len(ran))]
#     return (a * x + b) % p


start_time = time.time()

sc = SparkContext("local[*]", "inf553_hw3")
data = sc.textFile(sys.argv[1]).cache()
header = data.first()
new_data = data.filter(lambda record: record != header).map(lambda record: record.split(",")).filter(
    lambda x: len(x) == 3).cache()

user = new_data.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).keys()
business = new_data.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).keys()

user_num = user.count()
business_num = business.count()

user_map = {}
business_map = {}


for user_id, user_name in enumerate(user.collect()):
    user_map[user_name] = user_id

for business_id, business_name in enumerate(business.collect()):
    business_map[business_name] = business_id


band = 20
row = 5
n = band * row


sing_list = []

# singnature_matrix = []


# while n != 0:
ran = [71, 277, 691, 1699, 2087, 4211, 6329, 9043, 12613]
a = random.randint(2, 100)
b = random.randint(2, 100)
p = ran[random.randrange(0, len(ran))]
single_line = new_data.map(lambda x: (x[1],  (a * user_map[x[0]] + b) % p)).groupByKey().mapValues(list).map(lambda x: (x[0], min(x[1])))
sing_list.append(single_line.collect())

singnature_matrix = single_line

a = random.randint(2, 100)
b = random.randint(2, 100)
p = ran[random.randrange(0, len(ran))]
single_line = new_data.map(lambda x: (x[1],  (a * user_map[x[0]] + b) % p)).groupByKey().mapValues(list).map(lambda x: (x[0], min(x[1])))
sing_list.append(single_line.collect())
# singnature_matrix = singnature_matrix.join(single_line).mapValues(list)

while n != 0:
    n -= 1
    a = random.randint(2, 100)
    b = random.randint(2, 100)
    p = ran[random.randrange(0, len(ran))]
    single_line = new_data.map(lambda x: (x[1],  (a * user_map[x[0]] + b) % p)).groupByKey().mapValues(list).map(lambda x: (x[0], min(x[1])))
    sing_list.append(single_line.collect())
    # singnature_matrix = singnature_matrix.join(single_line).mapValues(list).map(lambda x: comlist(x))
# print(single_line)

# print(len(single_line))

# singnature_matrix = singnature_matrix.collectAsMap()
# print(singnature_matrix)
# print(len(singnature_matrix))
# print(singnature_matrix["wZ_MKDImrS4EvbZ_YUWzfA"])


print(sing_list)
print(len(sing_list))


Duration = time.time() - start_time
print("Duration: " + str(Duration))