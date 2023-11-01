from configparser import ConfigParser
import psycopg2
import psycopg2.extras
import requests
import json


config = ConfigParser()
config.read("config.ini")



def connectDB():
    return psycopg2.connect(
        host=config['Database']["host"],
        database=config['Database']["db"],
        user=config['Database']["user"],
        password=config['Database']["password"])

def getdata(pname,key,post):
    try :
        return eval("post"+config[pname][key])
    except:
        if key=='mediaurl':
            return "Default_Article_JPG"
        if key=='summary':
            return eval("post"+config[pname]['title'])
        else:
            return None

def storetoDB(cursor,df):
    # cur.execute("PREPARE stmt")
    psycopg2.extras.execute_batch(
        cursor, 
        "INSERT INTO {} ({}) {} ON CONFLICT(id) DO UPDATE SET {} where article_master.pubdate<>Excluded.pubdate".format(
        "dev_performo.article_master",
        ",".join(df.columns),
        "VALUES({})".format(",".join(["%s" for _ in df.columns])),
        ",".join([i+"=EXCLUDED."+i for i in df.columns])), 
        df.values)
    # cur.execute("DEALLOCATE stmt")

def getTrendingKeywords():
    url = "https://trends.google.com/trends/api/realtimetrends?hl=en-US&tz=-330&cat=all&fi=0&fs=0&geo=IN&ri=300&rs=100&sort=0"
    response = requests.get(url)
    return json.loads(response.text[5:])
    
class ScaleSERP:
    api_key = ""
    def __init__(self,key):
        self.api_key = key
    
    def get_data(self,keyword):
        params = {
            'api_key': self.api_key,
            'q': keyword,
            'num' : '10',
            'location' : 'India'
        }
        return requests.get('https://api.scaleserp.com/search', params).json()

def getAPIKey():
    return config['SERP']["key"]