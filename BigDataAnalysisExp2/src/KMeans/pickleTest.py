import matplotlib.pyplot as plt
import pickle

# 从pickle文件读取降维结果

cluster_result = []
with open("part-r-00000") as f:
    for line in f:
        cluster_result.append(int(line.split('\t')[1].replace('\n',''))%100)
with open("usdata.pickle", "rb") as usdata:

    data = pickle.load(usdata)
    y = cluster_result[:10000]  # 这里，y表示聚类结果（一维向量）
    #y = [1,1,1,2,2,2,3]
    plt.scatter(data[:, 0], data[:, 1],s=10, c=y)
    plt.show()
