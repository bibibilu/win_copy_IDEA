import time
import sys
import random
import itertools
from pyspark import SparkContext

start = time.time()

sc = SparkContext("local[*]", "pyspark")
inputRDD = sc.textFile(sys.argv[1])

header = inputRDD.first()
data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(","))

user_businesses = data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list)
all_user = user_businesses.map(lambda x: x[0]).collect()
userNum = len(all_user)

business_users = data.map(lambda x: (x[1], x[0])).groupByKey()
all_business = business_users.map(lambda x: x[0]).collect()
businessNum = len(all_business)

dic_user = {}
for n, m in enumerate(all_user):
    dic_user[m] = n

dic_business = {}
for n, m in enumerate(all_business):
    dic_business[m] = n
# user_businesses_list = data.map(lambda x: (dic_user[x[0]], (x[1], 1))).groupByKey().mapValues(dict).sortByKey().collect()
business_users_dic = data.map(lambda x: (dic_business[x[1]], dic_user[x[0]])).groupByKey().mapValues(set).collectAsMap()

band = 20
row = 2
minhashNum = band * row
m = userNum
p_list = [53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593]

signature_matrix = []
for index in range(0, minhashNum):
    a = random.randint(0, businessNum)
    b = random.randint(0, businessNum)
    p = random.choice(p_list)
    signature = data.map(lambda x: (x[1], (a * dic_user[x[0]] + b) % p)).groupByKey().mapValues(list).map(lambda x: (x[0], min(x[1]))).collectAsMap()
    signature_matrix.append(signature)
    # for user_id, business in user_businesses_list:
    #     hash_value = (a*user_id+b) % p
    #     for business_id in business:
    #         # hash_value_list = []
    #         if business_id not in matrix:
    #             matrix[business_id] = hash_value
    #             # hash_value_list.append(hash_value)
    #         else:
    #             matrix[business_id] = min(matrix[business_id], hash_value)
    #             # hash_value_list.append(min(matrix[business_id], hash_value))

# print(signature_matrix)
#     for bus in all_business:
#         value = matrix[bus]
#         if bus not in signature_dic:
#             signature_dic[bus] = [value]
#         else:
#             signature_dic[bus].append(value)

# for bus in all_business:
#     signature_matrix.append((bus, signature_dic[bus]))

candidate_pair_list = []
for bandId in range(band):
    start_row = bandId * row
    end_row = (bandId + 1) * row
    bucket = {}
    for bID in dic_business:
        temp = 0
        for k in range(row):
            temp += signature_matrix[start_row + k][bID] * random.choice(p_list)
        hash_value = temp % 9066
        if hash_value not in bucket:
            bucket[hash_value] = [bID]
        else:
            bucket[hash_value].append(bID)
    for key in bucket:
        value = bucket[key]
        if len(value) > 1:
            candidate_pair = itertools.combinations(value, 2)
        for pair in candidate_pair:
            candidate_pair_list.append(tuple(sorted(pair)))

final_pair = []
for ele in candidate_pair_list:
    dis = len(business_users_dic[dic_business[ele[0]]] & business_users_dic[dic_business[ele[1]]])
    un = len(business_users_dic[dic_business[ele[0]]].union(business_users_dic[dic_business[ele[1]]]))
    sim = float(dis)/float(un)
    if sim >= 0.5:
        final_pair.append((ele[0], ele[1], str(sim)))
final_pair = sorted(final_pair)

output_file = open(sys.argv[2], "w")
output_file.write("business_id_1, business_id_2, similarity")
for i in final_pair:
    output_file.write(i[0] + i[1] + i[2] + "\n")
output_file.close()

print(len(final_pair))

end = time.time()
print("Duration: %s" % (end - start))

# candidate_pair_list = []
# band_dic = {}
# for i in signature_matrix:
#     for j in range(0, len(i[1]), row):
#         bandNo = j/row
#         # print(band_id)
#         band_key = (bandNo, i[1][j], i[1][j+1])
#         if band_key not in band_dic.keys():
#             band_dic[band_key] = []
#             band_dic[band_key].append(i[0])
#         else:
#             band_dic[band_key].append(i[0])
# # print(band_dic)
# for band_key in band_dic.keys():
#     if len(band_dic[band_key]) >= 2:
#         candidate = band_dic[band_key]
#         # print(candidate)
#         candidate_pair = itertools.combinations(sorted(candidate), 2)
#         for pair in candidate_pair:
#             candidate_pair_list.append(tuple(sorted(pair)))
#             intersection = dic_business_users[pair[0]] & dic_business_users[pair[1]]
#             union = dic_business_users[pair[0]] | dic_business_users[pair[1]]
#             similarity = len(intersection)/len(union)
#             if similarity >= 0.5:
#                 final_pair.append((pair[0], pair[1], similarity))

# candidate_pair = []
# for i in range(len(signature_matrix)):
#     business_i = signature_matrix[i][0]
#     signature_i = signature_matrix[i][1]
#     for j in range(i+1, len(signature_matrix)):
#         business_j = signature_matrix[j][0]
#         signature_j = signature_matrix[j][1]
#         flag = bandcomparison(signature_i, signature_j, b, r)
#         # for index in range(0, b-1):
#         #     flag = False
#         #     u = signature_i[index:index+r]
#         #     v = signature_j[index:index+r]
#         #     if u == v:
#         #         flag = True
#         #     index = index + r
#         if flag:
#             pair = [business_i, business_j]
#             candidate_pair.append(pair)











