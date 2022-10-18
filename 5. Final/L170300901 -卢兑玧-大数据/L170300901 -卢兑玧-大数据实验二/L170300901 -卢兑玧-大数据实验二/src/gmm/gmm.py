import matplotlib.pyplot as plt
import pickle
import numpy as np
from scipy.stats import multivariate_normal

pi = []
mu = []
sigma = []
count = 0


with open("Lab2/GMM/gmm_m_6/part-r-00000", "r") as f:
    for line in f.readlines():
        line = line.strip('\n')
        value = line.split('\t')
        if(np.abs(float(value[1])) < 1e-12):
            continue
        else:
            pi.append(float(value[1]))
            mu_t = [float(v) for v in value[2].split(',')]
            mu.append(mu_t)
            sigma_t = [v for v in value[3].split(';')]
            sigma_t_t = []
            for t in sigma_t:
                sigma_t_t.append(np.array([float(v) for v in t.split(',')]))
            sigma.append(sigma_t_t)
            count = count + 1

for i in range(count):
    print(i)
    print(pi[i])
    print(mu[i])
    print(np.array(sigma[i]).shape)
    print("======================================================")

print(np.sum(pi))

g = []
for i in range(count):
    g.append(multivariate_normal(mean=mu[i], cov=sigma[i] + np.eye(68) * 1e-6))

point = []

with open("Lab2/clusterFile/USCensus1990.data.txt", 'r') as f:
    c = 0
    for line in f.readlines():
        c = c + 1
        p = [float(x) for x in line.split(',')]
        del p[0]
        dis = float('-inf')
        cluster_id = 0
        for i in range(count):
            z = g[i].pdf(p)
            if z > dis:
                cluster_id = i
                dis = z
        point.append(cluster_id)
        if c >= 10000 :
            break
print(point)

with open('Lab2/usdata.pickle', 'rb') as usdata:
    data = pickle.load(usdata)
    print(type(data))
    plt.scatter(data[:, 0], data[:, 1], c=point)
    plt.show()