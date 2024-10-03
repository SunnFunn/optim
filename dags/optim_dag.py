import pendulum
import subprocess as sp
from airflow.decorators import dag, task
from datetime import timedelta

#from airflow.utils.email import send_email
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from configparser import ConfigParser

from time import *


start_date = pendulum.local(2024, 10, 3, 16, 30, 0)


@dag(
    schedule=timedelta(seconds=60),
    start_date=start_date,
    end_date=start_date.add(minutes=600),
    catchup=False,
    tags=["RailOptim"],
)
def optimization_taskflow():
    @task()
    def fetch():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/fetch_data.py',
                '--url="http://172.26.144.1:8000"',
                '--path="/opt/airflow/data"'],
               capture_output=True,
               text=True)

    @task()
    def exempt():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/exemptions.py',
                '--path="/opt/airflow/data"'],
               capture_output=True,
               text=True)

    @task()
    def prepare():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/prepare.py',
                '--path="/opt/airflow/data"'],
               capture_output=True,
               text=True)

    @task()
    def optimize():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/optimize.py',
                '--path="/opt/airflow/data"'],
               capture_output=True,
               text=True)

    @task()
    def send():
        local_time = strftime("%Y-%m-%d-%H-%M", localtime())
        file_name = f"OPZ_{local_time}.xlsx"

        cfg = ConfigParser()
        cfg.read("/opt/airflow/email.ini")

        # данные почтового сервиса из конфиг файла email.ini
        server = cfg.get("smtp", "server")
        port = cfg.get("smtp", "port")
        from_addr = cfg.get("smtp", "from_addr")
        passwd = cfg.get("smtp", "passwd")

        subject = cfg.get("smtp", "subject")
        to_addr = cfg.get("smtp", "to_addr")
        text = cfg.get("smtp", "text")
        file_path = cfg.get("smtp", "file_path")

        # формируем тело письма
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg.attach(MIMEText(text))
        msg["To"] = to_addr

        attachment = MIMEBase('application', "octet-stream")
        header = 'Content-Disposition', f'attachment; filename="{file_name}"'
        try:
            with open(file_path, "rb") as fh:
                data = fh.read()
            attachment.set_payload(data)
            encoders.encode_base64(attachment)
            attachment.add_header(*header)
            msg.attach(attachment)
        except IOError:
            print(f"Ошибка при открытии файла вложения {file_path}")

        try:
            smtp = smtplib.SMTP(server, port)
            smtp.starttls()
            smtp.ehlo()
            smtp.login(from_addr, passwd)
            smtp.sendmail(from_addr, to_addr, msg.as_string().encode('utf-8'))
        except smtplib.SMTPException as err:
            print('Что - то пошло не так...')
            raise err
        finally:
            smtp.quit()

    fetch()
    exempt()
    prepare()
    optimize()
    send()


optimization_taskflow()
