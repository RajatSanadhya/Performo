import requests
import json
from flask import Flask, request
import psycopg2

class ScaleSERP:
    """class to get data from Scale API"""
    api_key = ""
    def __init__(self,key):
        self.api_key = key

    def get_data(self,keyword):
        params = {
            'api_key': self.api_key,
            'q': keyword
        }
        return requests.get('https://api.scaleserp.com/search', params).json()

class DataBase():
    """class to open connection with DB and do necessary operations"""
    HOST = ""
    DB = ""
    USERNAME = ""
    PASSWORD = ""
    def __init__(self,host,db,user,pwd):
        self.HOST = host
        self.DB = db
        self.USERNAME = user
        self.PASSWORD = pwd

    def connect(self):
        self.conn = psycopg2.connect(host=self.HOST,
                                     database=self.DB,
                                     user=self.USERNAME,
                                     password=self.PASSWORD)
        self.cur = self.conn.cursor()

    def insert_data(self,data,table):
        query = f'INSERT INTO {table} VALUES(%s)'
        self.cur.execute(query, (json.dunps(data),))
        self.conn.commit()

    def insert_organic_results(self,data):
        self.insert_data(self,data["organic_results"],"organic_results")

    def insert_top_stories(self,data):
        self.insert_data(self,data["top_stories"],"top_stories")

    def disconnect():
        self.cur.close()
        self.conn.close()


data = ""
scale_serp = ScaleSERP("")
db = DataBase("localhost","test","rajat","rajat")
app = Flask(__name__)

@app.route('/api/keyword', methods=['POST'])
def keyword_data():
    global data,scale_serp,db
    data = scale_serp.get_data(request.json.get('keyword'))
    db.connect()
    db.insert_organic_results(data)
    db.insert_top_stories(data)
    db.disconnect()

if __name__ == '__main__':
    app.run()
