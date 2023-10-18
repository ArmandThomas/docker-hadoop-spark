from flask import Flask, jsonify, request
from hdfs import InsecureClient

app = Flask(__name__)

url = 'http://namenode:9870/'

client = InsecureClient(url)


@app.route('/data', methods=['GET'])
def get_data():

    file_path = "/data/openbeer/data/output/csv_inflation_bigmac.csv/part-00000-fe841e62-1d8d-46da-8975-041ec858f945-c000.csv"

    with client.read(file_path) as reader:
        content = reader.read()
        print(content)

    return content

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)