import smtplib
from os.path import basename
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

def mail_html(html, subj, to, fr, files=[]):
    # Create message container - the correct MIME type is multipart/alternative here!
    MESSAGE = MIMEMultipart('alternative')
    MESSAGE['subject'] = subj
    MESSAGE['To'] = to
    MESSAGE['From'] = fr
 
    # Record the MIME type text/html.
    HTML_BODY = MIMEText(html, 'html')
 
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    MESSAGE.attach(HTML_BODY)
 
    # Attach files as attachments
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(basename(f)))
        MESSAGE.attach(part)
            
    # The actual sending of the e-mail
    #server = smtplib.SMTP('202.102.62.76', 25)
    try:
        server = smtplib.SMTP('202.102.62.76', 25, timeout=30) # internet line
        server.set_debuglevel(1)
        server.sendmail(MESSAGE['From'], MESSAGE['To'].split(","), MESSAGE.as_string())
        server.quit()
    except smtplib.SMTPException:
        server = smtplib.SMTP('168.8.250.75', 25, timeout=30) # vpn line
        server.set_debuglevel(1)
        server.sendmail(MESSAGE['From'], MESSAGE['To'].split(","), MESSAGE.as_string())
        server.quit()    
