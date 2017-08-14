# Scrapy settings for dirbot project

SPIDER_MODULES = ['dirbot.spiders']
NEWSPIDER_MODULE = 'dirbot.spiders'
DEFAULT_ITEM_CLASS = 'dirbot.items.Website'


MYSQL_HOST = 'localhost'
MYSQL_DBNAME = 'njhouse'
MYSQL_USER = 'root'
MYSQL_PASSWD = 'root'
QUERY_TIME = 2


ITEM_PIPELINES = {
    'dirbot.pipelines.njhouse_pipeline': 301,
}