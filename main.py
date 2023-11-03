import feedparser
import pandas as pd
from datetime import datetime,timedelta
import time
from hashlib import md5
from base64 import b64encode
from Utils import connectDB,getdata,storetoDB,getTrendingKeywords,ScaleSERP,getAPIKey

publishers = [
#     (1,"BS"),
    (2,"CNBC"),
    (3,"DAJ"),
    (4,"FP"),
    (5,"HT"),
#     (6,"ITO"),
    (7,"ITE"),
    (8,"ITH"),
#     (9,"IE"),
    (10,"LM"),
    (11,"MC"),
    (12,"NDTV"),
    (13,"THIN"),
    (14,"TRIB"),
    (15,"TOI"),
    (16,"NEWS18")
]

count=0

conn = connectDB()
cursor = conn.cursor()
scale_serp = ScaleSERP(getAPIKey())

#### Fetching RSS feeds and storing them into DB
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

##### Fetching Trending keywords and storing them along with ranking and missed articles into DB
data = getTrendingKeywords()

for k in data['storySummaries']['trendingStories'][:5]:
    for j in k['entityNames']:
        if(not len(j)>0):
            continue
        dat = scale_serp.get_data(j)
        for i in dat["organic_results"]:
            cursor.execute(f"""INSERT INTO dev_performo.article_ranking 
                            (rank,rank_datetime,article_id,block_position) 
                            select {i['position']},'{datetime.now().isoformat()}',(select id from dev_performo.article_master am where link = '{i['link']}'),{i['block_position']}
                            where exists (select 1 from dev_performo.article_master am2 where link = '{i['link']}')""")
            cursor.execute(f"""INSERT INTO dev_performo.missed_article_ranking 
                            (rank,rank_datetime,link,block_position) 
                            select {i['position']},'{datetime.now().isoformat()}','{i['link']}',{i['block_position']}
                            where not exists (select 1 from dev_performo.article_master am2 where link = '{i['link']}')""")
        cursor.execute(
            f"""insert into dev_performo.trends_keywords 
                (name,firstseendate,lastseendate) 
                select '{j}','{datetime.now().isoformat()}','{datetime.now().isoformat()}' 
                where not exists ( select 1 from dev_performo.trends_keywords where name = '{j}');
                update dev_performo.trends_keywords 
                set lastseendate='{datetime.now().isoformat()}'
                where name='{j}';""")
        conn.commit()

cursor.close()
conn.close()