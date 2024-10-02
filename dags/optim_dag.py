import pendulum
import subprocess as sp
from airflow.decorators import dag, task
from datetime import timedelta


start_date = pendulum.local(2024, 10, 2, 10, 45, 0)


@dag(
    schedule=timedelta(seconds=5),
    start_date=start_date,
    end_date=start_date.add(minutes=120),
    catchup=False,
    tags=["RailOptim"],
)
def optimization_taskflow():
    @task()
    def fetch():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/fetch_data.py',
                '--url="http://172.26.144.1:8000"',
                '--path="/opt/airflow/app/data"'],
               capture_output=True,
               text=True)

    @task()
    def exempt():
        sp.run(['python3',
                '/opt/airflow/app/pipeline/exemptions.py',
                '--path="/opt/airflow/app/data"'],
               capture_output=True,
               text=True)

    fetch()
    exempt()


optimization_taskflow()
