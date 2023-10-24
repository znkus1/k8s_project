from datetime import datetime, timedelta
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "donghyun",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}
tz = pytz.timezone("Asia/Seoul")
dt = datetime.now(tz=tz)
year, month = dt.year, 12 if dt.month == 1 else dt.month - 1

# for test
# year, month = 2002, 8

fname = f"{year}{month:02}.csv"


def postgres_to_minio():
    # postgres_to_local
    import psycopg2

    class PostgresDB:
        def __init__(self, host, dbname, user, password, port=5432):
            self.conn = psycopg2.connect(
                host=host,
                dbname=dbname,
                user=user,
                password=password,
                port=port,
            )

        def execute(self, sql, data=None):
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute(sql, data)

        def execute_many(self, sql, data_list):
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        curs.executemany(sql, data_list)
            except Exception as e:
                self.conn.rollback()
                print(f"Error while bulk inserting: {e}")
                raise e

    postgres = PostgresDB("172.31.3.234", "dbname", "postgres", "postgres")
    sql = f"COPY (SELECT * FROM db WHERE db.Year={year} and db.month={month}) TO '{fname}' WITH CSV HEADER;"
    postgres.execute(sql)

    # local_to_minio
    from minio import Minio

    client = Minio("172.31.3.234:9000", "admin", "password", secure=False)
    client.list_buckets()
    client.fput_object('csv', fname, fname)


with DAG(
    dag_id="monthly_db_to_minio",
    default_args=default_args,
    start_date=datetime(2023, 10, 1, tzinfo=tz),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    task = PythonOperator(task_id="postgres_to_minio",
                          python_callable=postgres_to_minio)

    task