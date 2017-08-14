# -*- coding: utf-8 -*-
# __author__='Hanxiaoyang'
import jieba  # 分词包
import numpy  # numpy计算包
import codecs  # codecs提供的open方法来指定打开的文件的语言编码，它会在读取的时候自动转换为内部unicode
import pandas  # 数据分析包
import matplotlib.pyplot as plt
from wordcloud import WordCloud  # 词云包

file = codecs.open("D:\Devlop\Python\python\sunchengyu.txt", 'r')
content = file.read()
file.close()
print(content)
segment = []
segs = jieba.cut(content)  # 切词，“么么哒”才能出现
for seg in segs:
    if len(seg) > 1 and seg != '\r\n':
        segment.append(seg)
words_df = pandas.DataFrame({'segment': segment})
words_df.head()
stopwords = pandas.read_csv("D:\Devlop\Python\python\stopwords.txt", index_col=False, quoting=3, sep="\t",
                            names=['stopword'], encoding='gbk')
words_df = words_df[~words_df.segment.isin(stopwords.stopword)]
words_stat = words_df.groupby(by=['segment'])['segment'].agg({"计数": numpy.size})
words_stat = words_stat.reset_index().sort(columns="计数", ascending=True)
# print words_stat#打印统计结果
wordcloud = WordCloud(font_path="simhei.ttf", background_color="black")
wordcloud = wordcloud.generate_from_frequencies(words_stat)
plt.axis("off")
plt.imshow(wordcloud)
plt.show()
