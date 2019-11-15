from pyspark import SparkContext
import itertools
import sys, time


start = time.time()

sc = SparkContext("local[*]", "pyspark")
threshold = int(sys.argv[1])
inputRDD = sc.textFile(sys.argv[2])

header = inputRDD.first()
data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(","))
user_bID = data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)
user_bID_dic = user_bID.collectAsMap()

node_set = set()
pair_list_1 = []
pair_list_2 = []
user_pair = list(itertools.combinations(user_bID_dic.keys(), 2))
for pair in user_pair:
    overlap_bID = user_bID_dic[pair[0]].intersection(user_bID_dic[pair[1]])
    if len(overlap_bID) >= threshold:
        pair_list_1.append((pair[0], pair[1]))
        pair_list_2.append((pair[1], pair[0]))
        node_set.add(pair[0])
        node_set.add(pair[1])
user_edges_1 = sc.parallelize(pair_list_1).groupByKey().mapValues(set)
user_edges_2 = sc.parallelize(pair_list_2).groupByKey().mapValues(set)
adjacency_list = user_edges_1.union(user_edges_2).reduceByKey(lambda x, y: x.union(y))
adjacency_dic = adjacency_list.collectAsMap()

nodeNum = adjacency_list.count()


def compute_betweeness(root):
    levels_dic = {}
    predecessors = {}
    successors = {}
    node_credit = {}

    level = 1
    levels_dic[level] = set()
    levels_dic[level].add(root)

    next_level = adjacency_dic[root]
    node_credit[root] = 1

    while len(next_level) != 0:
        level += 1
        levels_dic[level] = next_level
        connect_node_set = set()
        for next_node in next_level:
            node_credit[next_node] = 1
            for child in adjacency_dic[next_node]:
                connect_node_set.add(child)
        next_level = connect_node_set - levels_dic[level-1] - levels_dic[level]

    temp_betweeness = {}
    for levelNo in range(max(levels_dic.keys()), 0, -1):
        if levelNo != 1:
            for node in levels_dic[levelNo]:
                successors[node] = adjacency_dic[node] - levels_dic[levelNo-1] - levels_dic[levelNo]
                predecessors[node] = levels_dic[levelNo-1].intersection(adjacency_dic[node])
                if len(successors[node]) == 0:
                    temp_credit = node_credit[node]/len(predecessors[node])
                else:
                    betweeness = 0
                    for child in successors[node]:
                        down_edge = (min(child, node), max(child, node))
                        betweeness += temp_betweeness[down_edge]
                    node_credit[node] = betweeness + 1
                    temp_credit = node_credit[node]/len(predecessors[node])
                for father in predecessors[node]:
                    up_edge = (min(node, father), max(node, father))
                    if up_edge not in temp_betweeness:
                        temp_betweeness[up_edge] = temp_credit
                    else:
                        temp_betweeness[up_edge] += temp_credit

    final_betweeness = {}
    betweeness_dic = {}
    for edge in temp_betweeness:
        final_betweeness[edge] = temp_betweeness[edge]/2
        if temp_betweeness[edge]/2 not in betweeness_dic:
            betweeness_dic[temp_betweeness[edge]/2] = []
            betweeness_dic[temp_betweeness[edge]/2].append(sorted(edge))
        else:
            betweeness_dic[temp_betweeness[edge]/2].append(sorted(edge))

    for betweeness in betweeness_dic:
        for edge_list in betweeness_dic[betweeness]:
            for edge in edge_list:
                yield (betweeness, edge)


betweeness_edge = adjacency_list.flatmap(lambda x: compute_betweeness(x[0])).sortByKey(ascending=False).sortBy(lambda x: x[1]).collect()
edge_betweeness = adjacency_list.flatmap(lambda x: compute_betweeness(x[0])).map(lambda x: (x[1], x[0])).sortByKey().sortBy(lambda x: x[0])
adjacency_matrix = {}
# edgeNum = adjacency_matrix.keys()
output_file_1 = open(sys.argv[3], "w")
for i in edge_betweeness:
    adjacency_matrix[i[1]] = 1
    output_file_1.write("(‘user_id1’, ‘user_id2’), betweenness value\n")
    output_file_1.write(i[1] + "," + i[0] + "\n")
output_file_1.close()

end = time.time()
print("Duration: %s" % (end - start))

# max_betweeness = edge_betweeness.map(lambda x: x[0]).first()
# cut_edges = edge_betweeness.filter(lambda x: x[0] == max_betweeness).mapValues.(set).map(lambda x: x[1]).collect()

# def caculate_modularity(community_list):
#     Q = 0
#     for community in community_list:
#         for node_i in community:
#             for node_j in community:
#                 edge = (min(node_i, node_j), max(node_i, node_j))
#                 # aij = adjacency_dic[]
#                 if edge in adjacency_matrix:
#                     temp = 1 - len(adjacency_dic[node_i]*len(adjacency_dic[node_j]))/2*
#                 else:
#                     temp = - len(adjacency_dic[node_i])*len(adjacency_dic[node_j])
#                 Q = Q + temp

# def community():
#     for edge in cut_edges:
#         adjacency_dic[edge[0]].remove(edge[1])
#         adjacency_dic[edge[1]].remove(edge[0])



