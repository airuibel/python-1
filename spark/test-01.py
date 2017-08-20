# -*- coding:utf8 -*-
import networkx as nx
import matplotlib.pyplot as plt

G = nx.Graph()
edgelist = []
with open('F:\\test1.del', mode='rb') as f:
    for line in f:
        data = line.decode('gbk', 'ignore').strip().split("|")
        if data[0] is not None and data[1] is not None and len(data[0]) > 0 and len(data[1]) > 0:
            # print(data[0], data[1])
            G.add_edge(data[0], data[1])
            #edgelist.append((data[0], data[1]))

# edgelist = [('a', 'c'), ('a', 'e'), ('b', 'a'), ('c', 'd'), ('d', 'b'), ('e', 'b')]
#G.add_edges_from(edgelist)
try:
    nx.cycle_basis(G)
except:
    pass
print(list(nx.cycle_basis(G)))
