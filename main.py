import time
start = time.time()
import multiprocessing
import feedparser
import pandas as pd
from datetime import datetime,timedelta
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
conn = connectDB()
cursor = conn.cursor()
scale_serp = ScaleSERP(getAPIKey())

def process_data(pub):
    pid=pub[0][0]
    pname=pub[0][1]
    count=0
    conn = connectDB()
    cursor = conn.cursor()
    cursor.execute(f"""select
            publisher_salt,id, feed_url
            from dev_performo.publisher_category_mapping pcm
            where publisher_id = {pid}""")
    print(f'{datetime.now()} - started fetching feed for publisher : {pname}')
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
                                    (datetime.fromtimestamp(time.mktime(getdata(pname,"pubdate",post)))+timedelta(hours=5,minutes=30)).isoformat(),
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
    print(f'{datetime.now()} - data fetching for publisher : {pname} || {df.shape[0]} articles fetched ||')
    storetoDB(cursor,df,pname)
    conn.commit()
    cursor.close()
    conn.close()



#### Fetching RSS feeds and storing them into DB
if __name__ == '__main__':
    with multiprocessing.Pool() as pool:
        pool.map(process_data, list(zip(publishers)))
    ##### Fetching Trending keywords and storing them along with ranking and missed articles into DB
    print(f'{datetime.now()} - fetching trending keywords')
    data = getTrendingKeywords()
    conn = connectDB()
    cursor = conn.cursor()
    print(f'{datetime.now()} - fetching results for each keyword')
    for k in data['storySummaries']['trendingStories'][:5]:
        for j in k['entityNames']:
            if(not len(j)>0):
                continue
            dat = scale_serp.get_data(j)
            try:
                for i in dat["news_results"]:
                    cursor.execute(f"""INSERT INTO dev_performo.article_ranking
                                    (rank,rank_datetime,article_id,block_position)
                                    select {i['position']},'{datetime.now().isoformat()}',(select id from dev_performo.article_master am where link = '{i['link']}' order by pubdate DESC limit 1),{i['position']}
                                    where exists (select 1 from dev_performo.article_master am2 where link = '{i['link']}');
                                    INSERT INTO dev_performo.missed_article_ranking
                                    (rank,rank_datetime,link,block_position)
                                    select {i['position']},'{datetime.now().isoformat()}','{i['link']}',{i['position']}
                                    where not exists (select 1 from dev_performo.article_master am2 where link = '{i['link']}')""")
                cursor.execute(
                    f"""insert into dev_performo.trends_keywords
                        (name,firstseendate,lastseendate)
                        select '{j}','{datetime.now().isoformat()}','{datetime.now().isoformat()}'
                        where not exists (select 1 from dev_performo.trends_keywords where name = '{j}' limit 1)
    	                   or (select lastseendate from dev_performo.trends_keywords where name = '{j}' order by lastseendate desc limit 1) <  NOW() - interval '35 Minutes';
                        update dev_performo.trends_keywords
                        set lastseendate='{datetime.now().isoformat()}'
                        where name='{j}' and lastseendate >  NOW() - interval '35 Minutes';""")
                conn.commit()
            except Exception as e:
                if(dat["request_info"]["success"]==False and dat["request_info"]["credits_remaining"]==0):
                    print(f'{datetime.now()} –– !!!Credits Expired!!! –– Credits Used : {dat["request_info"]["credits_used"]} –– Credits Remaining : {dat["request_info"]["credits_remaining"]}')
                    exit()
                else:
                    raise e
    print(f'{datetime.now()} - keywords and article ranks stored')
    cursor.close()
    conn.close()
    end = time.time()
    print(f'{datetime.now()} - Script ran in time {end-start}')
