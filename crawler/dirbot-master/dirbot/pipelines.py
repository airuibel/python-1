# -*- coding: utf-8 -*-
import pymysql
from dirbot.items import Website
from scrapy import log
from dirbot import settings
import datetime


class njhouse_pipeline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host=settings.MYSQL_HOST,
            db=settings.MYSQL_DBNAME,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASSWD,
            charset='utf8',
            use_unicode=True)
        self.turn = 0
        self.cursor = self.connect.cursor()
        self.cursor.execute("""select q_times from query_times""")
        self.n = self.cursor.fetchone()[0]
        self.cursor.execute("""delete from njhouse where turn = %s""", (self.n))
        if self.n == settings.QUERY_TIME:
            self.cursor.execute("""update query_times set q_times = 1""")
        else:
            self.cursor.execute("""update query_times set q_times = (q_times + 1)""")
        self.connect.commit()

    def process_item(self, item, spider):
        if item.__class__ == Website:
            try:
                curTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.cursor.execute(
                    """insert into njhouse(program_name,license_no,area,open_time,program_type,sale_phone_no,program_addr,etl_date, turn)values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (item['program_name'],
                     item['license_no'],
                     item['area'],
                     item['open_time'],
                     item['program_type'],
                     item['sale_phone_no'],
                     item['program_addr'],
                     curTime,
                     self.n))
                self.connect.commit()
            except Exception as error:
                log(error)
            return item
