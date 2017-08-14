from scrapy.spider import Spider
from scrapy.selector import Selector

from dirbot.items import Website
from dirbot.spiders.send_mail import send_email


class DmozSpider(Spider):
    name = "njhouse"
    allowed_domains = ["njhouse.com.cn"]
    start_urls = [
        "http://www.njhouse.com.cn/2016/spf/list.php?dist=&use=&saledate=5&pgno=1"
    ]

    def parse(self, response):
        sel = Selector(response)
        sites = sel.xpath('//table/tbody')
        for site in sites:
            item = Website()
            item['program_name'] = site.select('./tr[1]/td[3]/a/text()').extract()
            item['license_no'] = site.select('./tr[1]/td[5]/a/text()').extract()
            item['area'] = site.select('./tr[2]/td[2]/text()').extract()
            item['open_time'] = site.select('./tr[2]/td[4]/text()').extract()
            item['program_type'] = site.select('./tr[3]/td[2]/text()').extract()
            item['sale_phone_no'] = site.select('./tr[3]/td[4]/text()').extract()
            item['program_addr'] = site.select('./tr[4]/td[2]/text()').extract()
            yield item
        sm = send_email()
        sm.send_email()
        
