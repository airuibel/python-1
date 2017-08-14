import smtplib
import email.mime.multipart
import email.mime.text


class SendEmail(object):
    def __init__(self, msgfrom, msgfrompwd, msgto, msgsubject, msgcontent, msggate):
        self.msgfrom = msgfrom
        self.msgfrompwd = msgfrompwd
        self.msgto = msgto
        self.msgsubject = msgsubject
        self.msgcontent = msgcontent
        self.msggate = msggate

    def send_email(self):
        msg = email.mime.multipart.MIMEMultipart()
        msg['from'] = self.msgfrom  # 'bumpink@126.com'
        msg['to'] = self.msgto  # 'cysuncn@a126.com'
        msg['subject'] = self.msgsubject #'nationalIC'
        if self.msgcontent is not None:
            txt = email.mime.text.MIMEText(str(self.msgcontent))
            msg.attach(txt)
            smtp = smtplib.SMTP()
            smtp.connect(self.msggate, '25')  # 'smtp.126.com'
            smtp.login(self.msgfrom, self.msgfrompwd)
            smtp.sendmail(self.msgfrom, self.msgto, str(msg))
            smtp.quit()
