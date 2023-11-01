import feedparser
import pandas as pd
from datetime import datetime,timedelta
import time
from hashlib import md5
from base64 import b64encode
from Utils import connectDB,getdata,storetoDB

publishers = [
#    (1,"BS"),
#    (2,"CNBC"),
#    (3,"DAJ"),
#    (4,"FP"),
    (5,"HT"),
#    (6,"ITO"),
#    (7,"ITE"),
    (8,"ITH"),
#    (9,"IE"),
#    (10,"LM"),
#    (11,"MC"),
    (12,"NDTV"),
#    (13,"THIN"),
    (14,"TRIB"),
#    (15,"TOI"),
    (16,"NEWS18")
]

count=0

conn = connectDB()
cursor = conn.cursor()
for pid,pname in publishers:
    cursor.execute(f"""select 
            publisher_salt,id, feed_url 
            from dev_performo.publisher_category_mapping pcm 
            where publisher_id = {pid}""")
    data = cursor.fetchall()
    df = pd.DataFrame(columns=['id', 'title', 'pubdate','link', 'pub_category_id', 'author','guid', 'summary', 'mediaurl'])
    for pub_id,idx,url in data:
        feed = feedparser.parse(url)
        for post in feed["entries"]:
            try:
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            [
                                [
                                    str(pub_id)+"-"+b64encode(md5(getdata(pname,"id",post).encode('utf-8')).digest()).decode("utf-8"),
                                    getdata(pname,"title",post), 
                                    datetime.fromtimestamp(time.mktime(getdata(pname,"pubdate",post))).isoformat()+timedelta(hours=5,minutes=30),
                                    getdata(pname,"link",post),
                                    idx,
                                    getdata(pname,"author",post),
                                    getdata(pname,"guid",post),
                                    getdata(pname,"summary",post),
                                    getdata(pname,"mediaurl",post)
                                ]
                            ],
                            columns=df.columns)],
                    ignore_index=True)
            except Exception as e:
                count = count+1
    df["summary"] = df["summary"].str[:2047]
    storetoDB(cursor,df)
conn.commit()
conn.close()