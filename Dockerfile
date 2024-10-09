FROM apache/airflow:latest-python3.10
RUN airflow db init
RUN airflow users create --username alext --password admin --firstname Aleksey --lastname Tretyakov --role Admin --email alext2370@mail.ru

RUN pip install psycopg2-binary
RUN pip install apache-airflow-providers-postgres

#COPY --chown=airflow:root ./app/ ./app/
#COPY --chown=airflow:root ./data/ ./data/
#COPY --chown=airflow:root ./dags/*.py ./dags/
#COPY --chown=airflow:root requirements.txt .
#COPY --chown=airflow:root email.ini .
COPY --chown=airflow:root . .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["airflow", "standalone"]
