from flask import Flask, jsonify, request
from hdfs import InsecureClient

app = Flask(__name__)

url = 'http://namenode:9870/'

try :
    client = InsecureClient(url)
except Exception as e:
    print(e)

@app.route('/data', methods=['GET'])
def get_data():

    file_path = "/data/openbeer/data/output/csv_inflation_bigmac.csv"

    with client.read(file_path) as reader:
        content = reader.read()
        print(content)

    return content

if __name__ == 'main':
    app.run(host='0.0.0.0', port=8000)