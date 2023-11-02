from datetime import datetime, timedelta, timezone
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "donghyun",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
tz = pytz.timezone("Asia/Seoul")
dt = datetime.now(tz=tz)
year, month = dt.year, 12 if dt.month == 1 else dt.month - 1

# for test
# year, month = 1987, 10

fname = f"{year}{month:02}.csv"


def check_exist():
    from minio import Minio
    client = Minio("172.31.3.234:9000", "admin", "password", secure=False)
    for obj in client.list_objects("csv"):
        if fname == obj.object_name:
            return "complete"
    return "postgres_to_minio"

def postgres_to_minio():
    import pandas as pd
    import psycopg2

    # postgres_to_local
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

def failure_callback():
    import requests, time, hmac, hashlib, uuid, platform

    api_key = ""
    api_secret_key = ""
    phone = ''
    text = "task fails"

    url = "https://api.solapi.com/messages/v4/send"
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = timedelta(seconds=-utc_offset_sec)
    date = datetime.now().replace(tzinfo=timezone(offset=utc_offset)).isoformat()
    salt = str(uuid.uuid1().hex)
    signature = hmac.new(api_secret_key.encode(), (date + salt).encode(), hashlib.sha256).hexdigest()
    header = {
        'Authorization': 'HMAC-SHA256 ApiKey=' + api_key + ', Date=' + date + ', salt=' + salt + ', signature=' + signature,
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {'message': {'to': phone, 'from': phone, 'text': text}}
    data["agent"] = {"sdkVersion": "python/4.2.0", "osPlatform": platform.platform() + " | " + platform.python_version()}
    requests.post(url, headers=header, json=data)
    return

with DAG(
    dag_id="monthly_db_to_minio",
    default_args=default_args,
    start_date=datetime(2023, 10, 1, tzinfo=tz),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    op_check_exist = BranchPythonOperator(task_id="check_exist", 
                                          python_callable=check_exist,
                                          on_failure_callback=failure_callback,
                                          )

    op_postgres_to_minio = PythonOperator(task_id="postgres_to_minio",
                                          python_callable=postgres_to_minio,
                                          on_failure_callback=failure_callback,
                                          )

    op_complete = BashOperator(task_id="complete", depends_on_past=False, bash_command="echo complete")

    op_check_exist >> op_postgres_to_minio >> op_complete
    op_check_exist >> op_complete