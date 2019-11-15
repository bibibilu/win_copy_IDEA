import time
import sys
from itertools import combinations
from pyspark import SparkContext


def fun_1(baskets):
    freq_items = []
    count_1 = {}
    baskets = list(baskets)
    # result = []
    freq_itemset = []
    size = 1
    # num_basket = 0
    for basket in baskets:
        for item in basket:
            # temp.append(item)
            if str(item) not in count_1.keys():
                count_1[str(item)] = 1
            else:
                count_1[str(item)] += 1
        # num_basket += 1
    for item in count_1:
        if count_1[item] >= threshold:
            freq_items.append(item)
            yield (item, 1)
    # freq_items = [{item} for item in c ]
    freq_items = sorted(freq_items)
    freq_itemset.append(freq_items)
    # frequent.append(temp)

    size = 2
    count_2 = {}
    candidate_pair = []
    candidate_set = list(set(combinations(freq_items, size)))
    for pair in candidate_set:
        for basket in baskets:
            if set(pair).issubset(basket):
                if tuple(pair) not in count_2:
                    count_2[tuple(pair)] = 1
                else:
                    count_2[tuple(pair)] += 1

    for pair in count_2:
        if count_2[pair] >= threshold:
            candidate_pair.append(pair)
            yield (pair, 1)
    freq_items = sorted(candidate_pair)
    freq_itemset.append(freq_items)
    # print("\nFreq_itemset:\n", freq_itemset)

    size = 3
    while True:
        count_k = {}
        candidate_set = set([])
        # new_set = set(combinations(freq_items, 2))
        # print(new_set)
        for pairs in set(combinations(freq_items, 2)):
            sinset = set([])
            for pair in pairs:
                for ele in pair:
                    sinset.add(ele)
                    # print("\n:", sinset)
            if len(sinset) == size:
                candidate_set.add(tuple(sorted(sinset)))

        freq_items.clear()

        if len(candidate_set) == 0:
            break

        for candidate in candidate_set:
            for basket in baskets:
                if set(candidate).issubset(basket):
                    if tuple(candidate) not in count_k:
                        count_k[tuple(candidate)] = 1
                    else:
                        count_k[tuple(candidate)] += 1

        for candidate in count_k:
            if count_k[candidate] >= threshold:
                freq_items.append(candidate)
                yield (candidate, 1)

        freq_itemset.append(freq_items)
        size += 1

    # return freq_itemset


def fun_2(dataset, baskets):
    baskets = list(baskets)
    counts = {}
    for candidate in dataset:
        if type(candidate) != tuple:
            candidate = (candidate,)
        for basket in baskets:
            if set(candidate).issubset(basket):
                if candidate not in counts:
                    counts[candidate] = 1
                else:
                    counts[candidate] += 1
    # print(counts)
    # return counts.items()
    for k, v in counts.items():
        yield (k, v)


start = time.time()

sc = SparkContext("local", "pyspark")
caseNo = int(sys.argv[1])
support = int(sys.argv[2])
inputRDD = sc.textFile(sys.argv[3], 2)
# outputRDD = sc.textFile(sys.argv[4])

header = inputRDD.first()

if caseNo == 1:
    data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(",")).map(lambda x: ((x[0]), (x[1])))
elif caseNo == 2:
    data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(",")).map(lambda x: ((x[1]), (x[0])))

partitionNum = inputRDD.getNumPartitions()
threshold = support / 2

rdd2 = data.groupByKey().map(lambda x: set(x[1]))
allbaskets = rdd2.collect()
# print(rdd2.collect())

candidate_itemset = rdd2.mapPartitions(fun_1).reduceByKey(lambda x, y: x+y).keys()
for i in candidate_itemset.collect():
    print(i)

count_occurance = candidate_itemset.mapPartitions(lambda dataset: fun_2(dataset, allbaskets))
final_freq = count_occurance.filter(lambda x: x[1] >= support).keys()
# for i in final_freq.collect():
#     print(i)

output_file = open(sys.argv[4], "w")

print("Candidates:", file=output_file)
length_1 = {}
for each in candidate_itemset.collect():
    ele = each
    if type(ele) != tuple:
        ele = (ele,)
    if len(ele) not in length_1:
        length_1[len(ele)] = [tuple(ele)]
    else:
        length_1[len(ele)] += [tuple(ele)]

for each in length_1:
    length_1[each].sort()

for key in length_1.keys():
    v1 = length_1[key]
    if key == 1:
        st = ""
        for ele in v1:
            st += str(ele).replace(",", "") + ", "
        print(st.rstrip(", "), file=output_file)
    else:
        st = ""
        for i in range(0, len(v1)):
            st += str(v1[i])+", "
        print(st.rstrip(", "), file=output_file)
    print("", file=output_file)


print("Frequent Itemsets:", file=output_file)
length_2 = {}
for each in final_freq.collect():
    if len(each) not in length_2:
        length_2[len(each)] = [tuple(each)]
    else:
        length_2[len(each)] += [tuple(each)]

for each in length_2:
    length_2[each].sort()

for key in length_2.keys():
    v2 = length_2[key]
    if key == 1:
        st = ""
        for ele in v2:
            st += str(ele).replace(",", "") + ", "
        print(st.rstrip(", "), file=output_file)
    else:
        st = ""
        for i in range(0, len(v2)):
            st += str(v2[i])+", "
        print(st.rstrip(", "), file=output_file)   
    print("", file=output_file)


end = time.time()

print("Duration: %s" % (end - start))

output_file.close()
