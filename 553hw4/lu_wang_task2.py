from pyspark import SparkContext, SparkConf
import sys, time, itertools


start_time = time.time()
sc = SparkContext("local[*]", "inf553_hw4")

threshold = int(sys.argv[1])
data = sc.textFile(sys.argv[2])

# community_output = open(sys.argv[4], "w")


header = data.first()
data = data.filter(lambda record: record != header).map(lambda record: record.split(",")).cache()
user_all_business = data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set).collectAsMap()

graph_con = []
for con in itertools.combinations(user_all_business.keys(), 2):
    if len(user_all_business[con[0]] & user_all_business[con[1]]) >= threshold:
        graph_con.append(con)
        graph_con.append((con[1], con[0]))

edges = sc.parallelize(graph_con).map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)
user_onodes = edges.collectAsMap()


def order_node(a, b):
    return (min(a, b), max(a, b))


def Girvan_Newman(root):
    leveldic = {}
    parents = {}
    childs = {}
    head = root
    childs[head] = user_onodes[head]
    parents[head] = set([])
    leveldic[0] = {head}

    leveldic[1] = set([])
    for i in childs[head]:
        leveldic[1].add(i)

    leveldic[2] = set([])
    for i in leveldic[1]:
        parents[i] = {head}
        childs[i] = user_onodes[i] - leveldic[1] - leveldic[0]
        leveldic[2] = leveldic[2].union(childs[i])

    level_num = 2
    while len(leveldic[level_num]) != 0:
        leveldic[level_num+1] = set([])
        for i in leveldic[level_num]:
            parents[i] = leveldic[level_num-1].intersection(user_onodes[i])
            childs[i] = user_onodes[i] - leveldic[level_num] - leveldic[level_num-1]
            leveldic[level_num+1] = leveldic[level_num+1].union(childs[i])
        level_num += 1

    weight_nodes = {}
    weight_edges = {}
    for num in range(level_num, -1, -1):
        for node in leveldic[num]:
            if len(childs[node]) == 0:
                weight_nodes[node] = 1
                up_weight = float(weight_nodes[node])/ len(parents[node])
                for i in parents[node]:
                    weight_edges[order_node(node, i)] = up_weight
            else:
                weight = 0
                for i in childs[node]:
                    weight += weight_edges[order_node(i, node)]
                weight_nodes[node] = weight + 1
                if len(parents[node]) == 0:
                    continue
                up_weight = float(weight_nodes[node]) / len(parents[node])
                for i in parents[node]:
                    weight_edges[order_node(node, i)] = up_weight

    for pairs in weight_edges:
        yield (pairs, weight_edges[pairs])


betweenness = edges.flatMap(lambda x: Girvan_Newman(x[0])).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/2)).sortByKey().sortBy(lambda x: x[1], ascending = False).collect()

between_output = open(sys.argv[3], "w")
print("(‘user_id1’, ‘user_id2’), betweenness value", file=between_output)
for i in betweenness:
    print(i[0], ",", i[1], file=between_output)
between_output.close()


edges_num = len(betweenness)   #m
# print(edges_num)

adjacent_matrix = {}        #A
for i in betweenness:
    adjacent_matrix[i[0]] = 1

# print(adjacent_matrix)


communities = []


set_nodes = []
for i in user_onodes:
    set_nodes.append(user_onodes[i].union({i}))


def combine_set(node):
    community = user_onodes[node].union({node})
    for s in set_nodes:
        if len(s & community) != 0:
            community = community.union(s)
    yield (tuple(sorted(community)), 1)


nodes_set = edges.flatMap(lambda x: combine_set(x[0])).reduceByKey(lambda x, y: x+y).map(lambda x: x[0])







duration = time.time() - start_time
print("Duration: ", duration)

