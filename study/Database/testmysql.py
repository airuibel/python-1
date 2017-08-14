import pymysql
import settings
import smtplib
import email.mime.multipart
import email.mime.text
from prettytable import PrettyTable


class njhouse_pipeline(object):
    def __init__(self):
        self.connect = pymysql.connect(host=settings.MYSQL_HOST, db=settings.MYSQL_DBNAME, user=settings.MYSQL_USER,
                                       passwd=settings.MYSQL_PASSWD, charset='utf8', use_unicode=True)
        self.cursor = self.connect.cursor()

    def get_query_times(self):
        self.cursor.execute("""select * from njhouse n1 where turn = 2 """)
        rect = self.cursor.fetchall()
        # result = str(rect)
        self.cursor.close()
        self.connect.close()
        return rect


    def print_result(self):
        x = PrettyTable(
            ["program_name", "license_no", "area", "open_time", "program_type", "sale_phone_no", "program_addr",
             "etl_date", "turn"])
        x.align["program_name"] = "l"  # Left align city names
        x.padding_width = 1  # One space between column edges and contents (default)
        result = self.get_query_times()
        if result != ():
            for item in result:
                x.add_row(item)
            print(x)
        else:
            pass


if __name__ == "__main__":
    np = njhouse_pipeline()
    np.print_result()
