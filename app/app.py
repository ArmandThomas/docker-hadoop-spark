from flask import Flask, jsonify, request
from hdfs import InsecureClient

app = Flask(__name__)

url = 'http://namenode:9870/'

client = InsecureClient(url)


@app.route('/data', methods=['GET'])
def get_data():

    array_of_file = client.list('/data/openbeer/data/output/csv_agg_inflation_bigmac.csv')

    return jsonify(array_of_file)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)