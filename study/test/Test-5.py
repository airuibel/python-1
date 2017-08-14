# -*- coding: utf-8 -*-
"""
记录一年微信对话情况
"""
from wordcloud import WordCloud, ImageColorGenerator
import jieba
import matplotlib.pyplot as plt
from scipy.misc import imread
# 读取停用词
stopwords = {}
def stopword(filename=''):
    global stopwords
    f = open(filename, 'r')
    line = f.readline().rstrip()
    while line:
        stopwords.setdefault(line, 0)
        stopwords[line.decode('gbk')] = 1
        line = f.readline().rstrip()
    f.close()

stopword(filename='D:\Devlop\Python\python\stopwords.txt')

# 定义中文分词和停用词清洗
def cleancntxt(txt, stopwords):
    seg_generator = jieba.cut(txt, cut_all=False)
    seg_list = [i for i in seg_generator if i not in stopwords]
    seg_list = [i for i in seg_list if i != u' ']
    return (seg_list)

# 定义中文词云函数
def wordcloudplot(txt):
    wordcloud = WordCloud(font_path='simhei.ttf',
                          background_color="white",  # 可以选择black或white
                          margin=5, width=1800, height=800)  # 长宽度控制清晰程度?
    wordcloud = wordcloud.generate(txt)
    # Open a plot of the generated image.
    bimg = imread("D:\Devlop\Python\python\heart.png")
    bimgColors = ImageColorGenerator(bimg)
    plt.axis("off")
    plt.imshow(wordcloud)
    plt.show()

def plotTitleCloud(txtlist):
    txt = r' '.join(txtlist)
    seg_list = cleancntxt(txt, stopwords)
    # seg_list = jieba.cut(txt, cut_all=False)
    txt = r' '.join(seg_list)
    wordcloudplot(txt)

with open('D:\Devlop\Python\python\sunchengyu.txt') as f:
    t1 = f.readlines()
plotTitleCloud(t1)
