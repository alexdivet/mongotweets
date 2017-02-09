import json

from flask import Flask, request
from pymongo import MongoClient
from bson import json_util


app = Flask(__name__)


client = MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['twitter_collection']


def to_json(data):
    return json.dumps(data, default=json_util.default)


@app.route('/tweets', methods=['GET'])
def tweets():
    if request.method == 'GET':
        results = collection.find()
        json_results = []
        for result in results:
          json_results.append(result)
        return to_json(json_results)


if __name__ == '__main__':
    app.run(debug=True)
