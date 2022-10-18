import matplotlib.pyplot as plt
import pickle
import numpy as np

cluster_center = []
point = []
color = ['DC143C', '800080', '0000FF', '708090', '00CED1', '00FA9A', 'FFFF00', 'FFA500', 'FF0000', '008000', '00FFFF', '708090']

with open("KMeans_8_clusters/KMeans_8_Clusters_m_15/part-r-00000", "r") as center:
    for line in center.readlines():
        line = line.strip('\n')
        points = [float(x) for x in line.split('\t')[1].split(',')]
        cluster_center.append(points)


def distance(x, y):
    ans = 0
    for i in range(1, len(x)):
        ans =  ans + (x[i] - y[i]) * (x[i] - y[i])
    return ans

with open("clusterFile/USCensus1990.data.txt", 'r') as f:
    count = 0
    for line in f.readlines():
        count = count + 1
        p = [float(x) for x in line.split(',')]
        dis = float('inf')
        cluster_id = 0
        for i, l in enumerate(cluster_center):
            if distance(l, p) < dis:
                dis= distance(l, p)
                cluster_id = i
        point.append(cluster_id)
        if count >= 10000 :
            break
print(len(point))

with open('usdata.pickle', 'rb') as usdata:
    data = pickle.load(usdata)
    print(type(data))
    plt.scatter(data[:, 0], data[:, 1], c=point)
    plt.show()