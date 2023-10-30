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
# year, month = 1987, 10

fname = f"{year}{month:02}.csv"


def postgres_to_minio():
    # postgres_to_local
    import pandas as pd
    import psycopg2

    # 데이터베이스에 연결
    conn = psycopg2.connect(
        host="172.31.3.234",
        dbname="ml",
        user="postgres",
        password="postgres",
        port=5432,
    )

    # SQL 쿼리 실행
    query = f"SELECT * FROM airport WHERE airport.year='{year}' and airport.month='{month}'"
    df = pd.read_sql_query(query, conn)
    df = df.drop('id', axis=1)
    df = df.drop('insert_dt', axis=1)
    df.columns = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum', 'ActualElapsedTime', 'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest', 'Distance', 'TaxiIn', 'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']
    df.to_csv(fname, index=False)

    # 데이터베이스 연결 종료
    conn.close()

    # local_to_minio
    from minio import Minio

    client = Minio("172.31.3.234:9000", "admin", "password", secure=False)
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