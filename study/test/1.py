# encoding=utf-8
import jieba
import unittest

# seg_list = jieba.cut("我来到北京清华大学", cut_all=True)
# print ("Full Mode:", "/ ".join(seg_list))  # 全模式
# seg_list = jieba.cut("我来到北京清华大学", cut_all=False)
# print ("Default Mode:", "/ ".join(seg_list) ) # 精确模式
# seg_list = jieba.cut("　3月20日，南京医药发布公告称，因为信息披露存在违法违规行为被江苏省证监局依法处罚，南京医药及相关责任人被处以警告处分，并被处以数额不等的罚款。昨日(3月23日)，上海杰赛律师事务所王智斌律师对《每日经济新闻》记者表示，南京医药的行为已经涉及到虚假陈述，应对股民因此造成的损失进行赔偿。目前已经有数名投资者委托他们进行集体诉讼代理工作，相关手续正在办理过程中。延迟披露3项交易在20日发布的公告中，南京医药表示，经江苏省证监局认定，南京医药存在未披露放弃南京医药盐都有限公司(以下简称盐都医药)优先增资权事项、放弃南京医药国际健康产业有限公司 (以下简称南药国际)优先增资权事项和委托南药国际收购盐城恒健药业有限公司 (以下简称盐城恒健)86.825%股权事项等三项股权交易的行为。江苏省证监局查明，2010年，盐都医药净利润占南京医药净利润的74.5%，南药国际2010年净利润占南京医药净利润比例为44.1%，南京医药放弃盐都医药和南药国际优先增资权并分别于2011年10月18日与陕西和合医药系统投资管理有限公司(以下简称陕西和合)以及在2011年12月4日与江苏红石科技实业有限公司(以下简称红石科技)签订增资协议，导致南京医药失去了对上述两个子公司的控制权，对公司的资产、负债、权益和经营成果产生了重要影响。而南京医药委托南药国际收购盐城恒健86.825%股权，是南京医药的一项资产购买行为，盐城恒健2010年度86.825%股权相关的净利润为60.6万元，占南京医药最近一个会计年度经审计净利润的64.67%。上述股权收购行为均属于应当披露重大事件，应立即披露，但公司并未及时披露，而是于2012年3月7日进行了补充披露。上海热线理财频道")  # 默认是精确模式
# print (", ".join(seg_list))
# seg_list = jieba.cut_for_search("我来到北京清华大学")  # 搜索引擎模式
# print (", ".join(seg_list))
# #jieba cutword
#
# root = dict()
#
#
# def findLastNode(string):
#     node = root
#     index = 0
#     while index < len(string):
#         char = string[index]
#         if char in node:
#             node = node[char]
#         else:
#             break
#         index += 1
#     # print(index, node)
#     return (index, node)
#
#
# def insert(string):
#     index, node = findLastNode(string)
#     for char in string[index:]:
#         # print(char)
#         new_node = dict()
#         node[char] = new_node
#         node = new_node
#
#
# def printTree(node, layer):
#     if len(node) == 0:
#         return '\n'
#
#     rtns = []
#     items = sorted(node.items(), key = lambda x: x[0])
#     rtns.append(items[0][0])
#     rtns.append(printTree(items[0][1], layer + 1))
#     i = 0
#     for item in items[1:]:
#         rtns.append('.' * layer)
#         rtns.append(item[0])
#         rtns.append(printTree(item[1], layer + 1))
#
#     return ''.join(rtns)
#
#
# def toStr(root):
#     return printTree(root, 0)
#
#
# insert('sunchengyu')
#
# insert('sunsiqi')
# insert('sunchengyu12')
# insert('sunchengyu3')
# print(toStr(root))
import re
data = '谓“天人五衰”，是天神？最大的悲！哀。帝释是众天神的领袖'
print(re.split(" |！|？|。|，", data))
