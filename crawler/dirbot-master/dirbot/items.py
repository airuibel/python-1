from scrapy.item import Item, Field


class Website(Item):
    program_name = Field()
    license_no = Field()
    area = Field()
    open_time = Field()
    program_type = Field()
    sale_phone_no = Field()
    program_addr = Field()
