# -*- coding:utf8 -*-
import networkx as nx
import matplotlib.pyplot as plt

G = nx.DiGraph()
edgelist = []
with open('D:\Test\data\GUARANTEECIRCLE.del', mode='rb') as f:
    for line in f:
        data = line.decode('gbk', 'ignore').strip().split("|")
        if data[0] is not None and data[1] is not None and len(data[0]) > 0 and len(data[1]) > 0:
            # print(data[0], data[1])
            G.add_edge(data[0], data[1])
            # edgelist.append((data[0], data[1]))

# edgelist = [('a', 'c'), ('a', 'e'), ('b', 'a'), ('c', 'd'), ('d', 'b'), ('e', 'b')]
nx.draw(G, with_labels=True)
plt.show()
G.add_edges_from(edgelist)

print(list(nx.recursive_simple_cycles(G)))
