import sys, time, math
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

start = time.time()

sc = SparkContext("local[*]", "pyspark")
trainRDD = sc.textFile(sys.argv[1])
valRDD = sc.textFile(sys.argv[2])
caseNo = int(sys.argv[3])

header_train = trainRDD.first()
header_val = valRDD.first()
data_train = trainRDD.filter(lambda x: x != header_train).map(lambda x: x.split(",")).map(lambda x: ((x[0], x[1]), float(x[2]))).persist()
data_val = valRDD.filter(lambda x: x != header_val).map(lambda x: x.split(",")).map(lambda x: ((x[0], x[1]), float(x[2]))).persist()

testRDD = valRDD.filter(lambda x: x != header_val).map(lambda x: x.split(",")).map(lambda x: ((x[0], x[1]), 1)).persist()
data_true = data_train.subtractByKey(testRDD).persist()

if caseNo == 1:
    user_train = data_train.map(lambda x: x[0][0]).distinct().collect()
    user_val = data_val.map(lambda x: x[0][0]).distinct().collect()
    business_train = data_train.map(lambda x: x[0][1]).distinct().collect()
    business_val = data_val.map(lambda x: x[0][1]).distinct().collect()

    all_user = list(set(user_train) | set(user_val))
    all_business = list(set(business_train) | set(business_val))

    dic_user = {}
    user_dic = {}
    for n, m in enumerate(all_user):
        dic_user[m] = n
        user_dic[n] = m

    dic_business = {}
    business_dic = {}
    for n, m in enumerate(all_business):
        dic_business[m] = n
        business_dic[n] = m

    rating = data_val.map(lambda x: ((dic_user[x[0][0]], dic_business[x[0][1]]), float(x[1])))

    rating_val = rating.map(lambda x: (x[0][0], x[0][1]))
    rating_train = data_train.map(lambda x: Rating(dic_user[x[0][0]], dic_business[x[0][1]], float(x[1])))

    rank = 10
    numIterations = 10
    model = ALS.train(rating_train, rank, numIterations, 0.1)
    predictions = model.predictAll(rating_val).map(lambda r: ((r[0], r[1]), float(r[2])))
    ratesAndPreds = rating.map(lambda r: ((r[0][0], r[0][1]), r[1])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

    output_file = open(sys.argv[4], "w")

    output_file.write("user_id, business_id, prediction\n")
    for i in sorted(predictions.collect()):
        output_file.write(user_dic[i[0][0]] + "," + business_dic[i[0][1]] + "," + str(i[1]) + "\n")
    output_file.close()
    print("Root Mean Squared Error = " + str(math.sqrt(MSE)))

if caseNo == 2:
    user_average = data_true.map(lambda x: (x[0][0], (x[1], 1))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collectAsMap()
    user_normalize = data_true.map(lambda x: (x[0][0], (x[0][1], x[1]-user_average[x[0][0]]))).groupByKey().mapValues(dict).collectAsMap()
    uID_bID_true = data_true.map(lambda x: (x[0][0], x[0][1])).groupByKey().mapValues(set).collectAsMap()
    bID_uID_true = data_true.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(set).collectAsMap()
    pair_val = data_val.map(lambda x: (x[0][0], x[0][1]))
    uid_bidRating_val = data_val.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(dict).collectAsMap()

    similar_user = []
    prediction_list = []

    for pair in pair_val.collect():
        active_uID = pair[0]
        active_bID = pair[1]
        if active_bID not in bID_uID_true:
            prediction_list.append((pair, user_average[active_uID]))
        # elif active_uID not in uID_bID_true:
        #     prediction_list.append((pair, uid_bidRating_true[]))
        else:
            other_uID_list = bID_uID_true[active_bID]
            for other_uID in other_uID_list:
                if other_uID != active_uID:
                    bID_list_1 = uID_bID_true[active_uID]
                    bID_list_2 = uID_bID_true[other_uID]
                    bID_corated = bID_list_1.intersection(bID_list_2)
                    bID_rating_1 = user_normalize[active_uID]
                    bID_rating_2 = user_normalize[other_uID]
                    numerator1 = 0
                    d1 = 0
                    d2 = 0
                    for bID in bID_corated:
                        numerator1 += bID_rating_1[bID] * bID_rating_2[bID]
                        d1 += bID_rating_1[bID]**2
                        d2 += bID_rating_2[bID]**2
                    denominator1 = math.sqrt(d1) * math.sqrt(d2)
                    similarity = 0
                    if denominator1 != 0:
                        similarity = numerator1/denominator1
                    similar_user.append((similarity, other_uID))
            similar_user = sorted(similar_user)
            numerator2 = 0
            denominator2 = 0
            prediction_rating = 0
            if len(similar_user) > 10:
                for i in range(len(similar_user) - 10):
                    del similar_user[0]
            for neighbour in similar_user:
                if active_bID in user_normalize[neighbour[1]]:
                    numerator2 += user_normalize[neighbour[1]][active_bID] * neighbour[0]
                    denominator2 += abs(neighbour[0])
            if denominator2 == 0:
                prediction_rating = user_average[active_uID]
            else:
                prediction_rating = user_average[active_uID] + (numerator2 / denominator2)
                if prediction_rating > 5:
                    prediction_rating = 5
                elif prediction_rating < 0:
                    prediction_rating = 0
            prediction_list.append((pair, prediction_rating))

    count = 0
    diff = 0
    for i in prediction_list:
        count += 1
        diff += (i[1] - uid_bidRating_val[i[0][0]][i[0][1]])**2
    RMSE = math.sqrt(diff/count)
    print("Root Mean Squared Error1 = " + str(RMSE))

    output_file = open(sys.argv[4], "w")
    output_file.write("userID, businessID, rating\n")
    for i in prediction_list:
        output_file.write(i[0][0] + "," + i[0][1] + "," + str(i[1]) + "\n")
    output_file.close()

if caseNo == 3:
    business_average = data_true.map(lambda x: (x[0][1], (x[1], 1))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collectAsMap()
    business_normalize = data_true.map(lambda x: (x[0][1], (x[0][0], x[1]-business_average[x[0][1]]))).groupByKey().mapValues(dict).collectAsMap()

    bid_uidRating_true = data_true.map(lambda x: (x[0][1], (x[0][0], x[1]))).groupByKey().mapValues(dict).collectAsMap()
    bID_uID_true = data_true.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(set).collectAsMap()
    uID_bID_true = data_true.map(lambda x: (x[0][0], x[0][1])).groupByKey().mapValues(set).collectAsMap()
    bid_uidRating_val = data_val.map(lambda x: (x[0][1], (x[0][0], x[1]))).groupByKey().mapValues(dict).collectAsMap()
    pair_val = data_val.map(lambda x: (x[0][1], x[0][0]))

    similar_business = []
    prediction_list = []
    for pair in pair_val.collect():
        active_bID = pair[0]
        active_uID = pair[1]
        if active_uID not in uID_bID_true:
            # prediction_list.append((pair, business_average[active_bID]))
            prediction_list.append((pair, 3))
        elif active_bID not in bID_uID_true:
            prediction_list.append((pair, 3))
        else:
            other_bID_list = uID_bID_true[active_uID]
            for other_bID in other_bID_list:
                if other_bID != active_bID:
                    uID_list_1 = bID_uID_true[active_bID]
                    uID_list_2 = bID_uID_true[other_bID]
                    uID_corated = uID_list_1.intersection(uID_list_2)
                    uID_rating_1 = business_normalize[active_bID]
                    uID_rating_2 = business_normalize[other_bID]
                    numerator1 = 0
                    d1 = 0
                    d2 = 0
                    for uID in uID_corated:
                        numerator1 += uID_rating_1[uID] * uID_rating_2[uID]
                        d1 += uID_rating_1[uID]**2
                        d2 += uID_rating_2[uID]**2
                    denominator1 = math.sqrt(d1) * math.sqrt(d2)
                    similarity = 0
                    if denominator1 != 0:
                        similarity = numerator1/denominator1
                    similar_business.append((similarity, other_bID))
            similar_business = sorted(similar_business)
            numerator2 = 0
            denominator2 = 0
            prediction_rating = 0
            if len(similar_business) > 5:
                for i in range(len(similar_business) - 5):
                    del similar_business[0]
            for neighbour in similar_business:
                if active_uID in bid_uidRating_true[neighbour[1]]:
                    numerator2 += bid_uidRating_true[neighbour[1]][active_uID] * neighbour[0]
                    denominator2 += abs(neighbour[0])
            if denominator2 == 0:
                prediction_rating = business_average[active_bID]
            else:
                prediction_rating = numerator2 / denominator2
                if prediction_rating > 5:
                    prediction_rating = 5
                elif prediction_rating < 1:
                    prediction_rating = 1
            prediction_list.append((pair, prediction_rating))

    output_file = open(sys.argv[4], "w")
    output_file.write("userID, businessID, rating\n")
    for i in prediction_list:
        output_file.write(i[0][0] + "," + i[0][1] + "," + str(i[1]) + "\n")
    output_file.close()

    count = 0
    diff = 0
    for i in prediction_list:
        count += 1
        diff += (i[1] - bid_uidRating_val[i[0][0]][i[0][1]])**2
    RMSE = math.sqrt(diff/count)
    print("Root Mean Squared Error = " + str(RMSE))

end = time.time()
print("Duration: %s" % (end - start))
