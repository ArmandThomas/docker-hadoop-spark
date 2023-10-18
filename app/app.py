from flask import Flask, jsonify, request
from hdfs import InsecureClient

app = Flask(__name__)

@app.route('/data', methods=['GET'])
def get_data():

    file_path = "/data/openbeer/data/output/csv_inflation_bigmac.csv"

if __name__ == 'main':
    app.run(host='0.0.0.0', port=8000)