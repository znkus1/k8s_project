from flask import Flask, request, jsonify, render_template
from utils.spark import spark, loaded_lrModel, pipeline_model
from utils.minio import download_model


app = Flask(__name__)

# 정적 파일과 템플릿 파일의 경로 설정
app.static_folder = 'static'
app.template_folder = 'templates'


@app.route('/')
def input_page():
    return render_template('index.html')


@app.route('/update_model')
def update_model():
    download_model()
    return "model updated"


@app.route('/test', methods=['POST', 'GET'])
def test():
    return "why no?"


@app.route('/predict', methods=['POST'])
def predict():
    try:
        data = request.get_json()
        data = pipeline_model.transform(spark.createDataFrame([data]))
        predictions = loaded_lrModel.transform(data)
        result = round(predictions.collect()[0].predArrDelay, 5)
        return jsonify({'result': result})
    except Exception:
        msg = '출발지/도착지/항공사를 찾을 수 없습니다'
        return jsonify({'result': msg})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
