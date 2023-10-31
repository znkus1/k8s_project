import os
from minio import Minio


bucket_name = "spark-models"
local_folder = "spark_model"


def download_model():
    # 환경 변수에서 값을 가져옴
    minio_server = os.getenv("MINIO_SERVER_IP", "13.209.225.84")
    minio_port = os.getenv("MINIO_PORT", "9000")
    minio_id = os.getenv("MINIO_ID", "admin")
    minio_password = os.getenv("MINIO_PASSWORD", "password")

    # MinIO 클라이언트 생성
    client = Minio(f"{minio_server}:{minio_port}", minio_id, minio_password, secure=False)
    objects = client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        # 로컬 경로 생성
        local_path = os.path.join(local_folder, obj.object_name)
        local_dir = os.path.dirname(local_path)

        # 로컬에 디렉토리가 없다면 생성
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # 파일 다운로드
        client.fget_object(bucket_name, obj.object_name, local_path)
