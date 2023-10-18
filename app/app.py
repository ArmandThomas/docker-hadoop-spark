from flask import Flask, jsonify, request
from hdfs import InsecureClient
import pandas as pd

app = Flask(__name__)

url = 'http://namenode:9870/'

client = InsecureClient(url)


@app.route('/data', methods=['GET'])
def get_data():

    array_of_file = client.list('/data/openbeer/data/output/csv_agg_inflation_bigmac.csv')
    file_to_read = array_of_file[1]

    with client.read('/data/openbeer/data/output/csv_agg_inflation_bigmac.csv/' + file_to_read, encoding='utf-8') as reader:
        pd_df = pd.read_csv(reader)

    return jsonify(pd_df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)