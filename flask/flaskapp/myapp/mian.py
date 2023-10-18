from flask import Flask, request, jsonify
app = Flask(__name__)
@app.route('/') 
def welcome():
    return 'HELLO, API SERVER'
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

