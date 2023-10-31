from flask import Flask, request, jsonify, render_template
from minio import Minio
import os

import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml import PipelineModel

# 환경 변수에서 값을 가져옴
minio_server = os.getenv("MINIO_SERVER_IP")
minio_port = os.getenv("MINIO_PORT")
minio_id = os.getenv("MINIO_ID")
minio_password = os.getenv("MINIO_PASSWORD")

# MinIO 클라이언트 생성
client = Minio(f"{minio_server}:{minio_port}", minio_id, minio_password, secure=False)

# 다운로드할 버킷 이름
bucket_name = "spark-models"

# 로컬 저장 경로
local_folder = "local_folder"

# MinIO 버킷에서 객체 목록 가져오기
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


spark = SparkSession.builder \
    .master("local") \
    .appName("Airline EDA") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# 저장된 모델 불러오기
loaded_lrModel = LinearRegressionModel.load("local_folder/model")
pipeline_model = PipelineModel.load("local_folder/pipeline")

app = Flask(__name__)

# 정적 파일과 템플릿 파일의 경로 설정
app.static_folder = 'static'
app.template_folder = 'templates'

@app.route('/')
def input_page():
    return render_template('index.html')

@app.route('/test', methods=['POST','GET'])
def test():
    return "why no?"

@app.route('/predict', methods=['POST'])
def predict():
    try:
        data = request.get_json()  # JSON 데이터를 받아옵니다.
        print(data)
        # from user input to spark df
        data = pipeline_model.transform(spark.createDataFrame([data]))
        print(data)
        # 불러온 모델로 예측하기
        predictions = loaded_lrModel.transform(data)
        print(predictions)
        result = round(predictions.collect()[0].predArrDelay,5)
        print(result)
        return jsonify({'result': result})  # 결과를 JSON 형식으로 반환
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)