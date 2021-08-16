# encoding: utf-8
# author TurboChang

import smtplib
from core.exception.related_exception import EmailException
from email.mime.text import MIMEText

host = "smtp.exmail.qq.com"
subject = u""
to_mail = ""
from_mail = ""
msg = MIMEText("""
<table width="800" border="0" cellspacing="0" cellpadding="4">
        <tr>
            <td bgcolor="CECFAD" headers="20" style="font-size: 14px">
                *差异数据
            </td>
        </tr>
        <tr>
            <td bgcolor="#EFEBDE" height="100" style="font-size: 13px">
                
            </td>
        </tr>
    </table>
""", "html", "utf-8")

msg['Subject'] = subject
msg['From'] = from_mail
msg['To'] = to_mail

try:
    server = smtplib.SMTP()
    server.connect(host, "465")
    server.starttls()
    server.login("clx@datapipeline.com", "passwd")
    server.sendmail(from_mail, to_mail, msg.as_string())
    server.quit()
    print("邮件发送成功.")
except:
    raise EmailException("邮件: {0} 发送失败".format(subject))