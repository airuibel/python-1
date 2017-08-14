#-*- coding:utf-8 -*-

from lxml import etree
from selenium import webdriver
import time

# 京东手机商品页面
url="http://item.jd.com/4461494.html"

# 下面的xslt是通过集搜客的谋数台图形界面自动生成的
xslt_root = etree.XML("""\
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >
<xsl:template match="/">
<商品>
<xsl:apply-templates select="//*[@id='itemInfo' and count(.//*[@id='summary-price']/div[position()=2]/strong/text())>0 and count(.//*[@id='name']/h1/text())>0]" mode="商品"/>
</商品>
</xsl:template>

<xsl:template match="//*[@id='itemInfo' and count(.//*[@id='summary-price']/div[position()=2]/strong/text())>0 and count(.//*[@id='name']/h1/text())>0]" mode="商品">
<item>
<价格>
<xsl:value-of select="*//*[@id='summary-price']/div[position()=2]/strong/text()"/>
<xsl:value-of select="*[@id='summary-price']/div[position()=2]/strong/text()"/>
<xsl:if test="@id='summary-price'">
<xsl:value-of select="div[position()=2]/strong/text()"/>
</xsl:if>
</价格>
<名称>
<xsl:value-of select="*//*[@id='name']/h1/text()"/>
<xsl:value-of select="*[@id='name']/h1/text()"/>
<xsl:if test="@id='name'">
<xsl:value-of select="h1/text()"/>
</xsl:if>
</名称>
</item>
</xsl:template>
</xsl:stylesheet>""")

# 使用webdriver.PhantomJS
browser=webdriver.PhantomJS()
browser.get(url)
#time.sleep(3)

transform = etree.XSLT(xslt_root)

# 执行js得到整个dom
html = browser.execute_script("return document.documentElement.outerHTML")
doc = etree.HTML(html)
print (doc)
# 用xslt从dom中提取需要的字段
result_tree = transform(doc)
print(result_tree)