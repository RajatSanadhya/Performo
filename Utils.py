from configparser import ConfigParser
import psycopg2
import psycopg2.extras


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