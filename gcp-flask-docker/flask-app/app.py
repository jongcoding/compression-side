from flask import Flask, jsonify
from flask_pymongo import PyMongo
import mysql.connector
import os

app = Flask(__name__)

# MongoDB 설정
app.config["MONGO_URI"] = f"mongodb://{os.environ.get('MONGO_HOST')}:27017/testdb"
mongo = PyMongo(app)

# MariaDB 설정
db = mysql.connector.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)

@app.route('/')
def home():
    return "Hello, Flask with MariaDB and MongoDB!"

@app.route('/mysql')
def mysql_test():
    cursor = db.cursor()
    cursor.execute("SELECT DATABASE();")
    result = cursor.fetchone()
    cursor.close()
    return jsonify({"current_database": result[0]})

@app.route('/mongodb')
def mongodb_test():
    user = mongo.db.users.find_one({"name": "test"})
    return jsonify({"user": user})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
