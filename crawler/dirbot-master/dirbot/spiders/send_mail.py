import pymysql
import smtplib
import email.mime.multipart
import email.mime.text
from prettytable import PrettyTable
from dirbot import settings


class send_email(object):
    def __init__(self):
        self.connect = pymysql.connect(host=settings.MYSQL_HOST, db=settings.MYSQL_DBNAME, user=settings.MYSQL_USER,
                                       passwd=settings.MYSQL_PASSWD, charset='utf8', use_unicode=True)
        self.cursor = self.connect.cursor()

    def get_query_times(self):

        self.cursor.execute(
            """select  distinct turn from njhouse where etl_date = (select  max(etl_date) from njhouse ) limit 1""")
        self.turn = self.cursor.fetchone()[0]
        if 1 == self.turn:
            last_time = 2
        else:
            last_time = 1
        self.cursor.execute("""select * from njhouse n1 where turn = %s and not exists 
                                      (select * from njhouse n2 where n2.turn = %s and n1.license_no = n2.license_no)""", (self.turn, last_time))
        rect = self.cursor.fetchall()
        # result = str(rect)
        self.cursor.close()
        self.connect.close()
        return rect

    def format_result(self):
        x = PrettyTable(["项目名称", "最新许可证号", "区属", "最新拟开盘时间", "项目类别", "销售热线", "项目地址", "更新时间", "更新轮次"])
        x.align["program_name"] = "l"  # Left align city names
        x.padding_width = 1  # One space between column edges and contents (default)
        result = self.get_query_times()
        if result != ():
            for item in result:
                x.add_row(item)
            return x
        else:
            return None

    def send_email(self):
        msg = email.mime.multipart.MIMEMultipart()
        msg['from'] = 'bumpink@126.com'
        msg['to'] = 'cysuncn@a126.com'
        msg['subject'] = 'njhouse'
        content = self.format_result()
        if content != None:
            txt = email.mime.text.MIMEText(str(content))
            msg.attach(txt)
            smtp = smtplib
            smtp = smtplib.SMTP()
            smtp.connect('smtp.126.com', '25')
            smtp.login('bumpink@126.com', '*')
            smtp.sendmail('bumpink@126.com', 'cysuncn@126.com', str(msg))
            smtp.quit()
