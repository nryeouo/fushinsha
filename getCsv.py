import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
import re
import requests
import time

today = dt.date.today()
zenhan = str.maketrans("１２３４５６７８９０","1234567890","")

token = "***CENSORED***"
auth = {"Authorization": token}

query = "unit_id:133089874031904245 全裸 OR 下半身露出 "
limit = "50"
url = "https://api.nordot.jp/v1.0/search/contentsholder/posts.list"

csvDir = "(Directory)"

cur = ""

# 月初からの検索
today = today.replace(day=1)
today_iso = dt.datetime.combine(today, dt.time(0,0,0)).isoformat()+"+09:00"
created_at_d1 = "created_at:>=" + today_iso
query += created_at_d1

parameter = {"query": query, "limit": limit}

def apiGet(page, cursor=""):
  para = parameter
  if (page >= 2) and (len(cursor) != 0):
    para["cursor"] = cursor

  r = requests.get(url, params = para, headers = auth)
  j = r.json()
  if j["paging"]["has_next"] == 1:
    c = j["paging"]["next_cursor"]
  else:
    c = ""

  return j, c

def parse(req):
  posts = []
  for post in req["posts"]:

    entry = {}
    aid    = post["id"]
    title  = post["title"].translate(zenhan)
    desc   = post["description"].translate(zenhan)
    pub_at = dt.datetime.strptime(post["published_at"], "%Y-%m-%dT%H:%M:%S+00:00")
    pub_at += dt.timedelta(hours=9) #Japan

    if len(desc) == 0:
      r = requests.get(url="https://this.kiji.is/"+str(aid))
      s = BeautifulSoup(r.text, "html.parser")
      desc = s.find(class_="ma__p").text.translate(zenhan)
      time.sleep(1)

    # 露出時刻
    if "ごろ" not in desc:
      naked = "AM 12:00"
    elif "分ごろ" not in desc:
      naked = re.sub(r".+(午[前後]1?[0-9])時ごろ.+", r"\1:00", desc)
    else:
      naked = re.sub(r".+(午[前後]1?[0-9])時([0-9]{1,2})分ごろ.+", r"\1:\2", desc)
    
    naked = naked.replace("午前", "AM ")
    naked = naked.replace("午後", "PM ")
    naked = re.sub(r"M 0:", "M 12:", naked)
    ntime = dt.datetime.strptime(naked, "%p %I:%M")

    #露出都道府県
    place = re.match(r"^（(.{1,3}?)）", title).group(1)

    #露出日
    tsuki = re.match(r".+、([0-9]{1,2})月.+", desc)
    nichi = re.match(r".+((?<![0-9])([0-9])(?![0-9])|[12][0-9]|3[01])日.+", desc)

    if tsuki != None:
      ndate = dt.date(pub_at.year, int(tsuki.group(1)), int(nichi.group(1)))
    else:
      ndate = dt.date(pub_at.year, pub_at.month, int(nichi.group(1)))
    
    if dt.date.today() - ndate < dt.timedelta(0):
      ndate = ndate.replace(year=pub_at.year - 1)

    #全裸かどうか
    if "全裸" in title:
      zenra = "全裸"
    else:
      zenra = "下半身露出"
    
    #事案の格納
    entry["article_id"] = aid
    entry["naked_at"] = dt.datetime(
      ndate.year, ndate.month, ndate.day,
      ntime.hour, ntime.minute, ntime.second
    )
    entry["created_at"] = pub_at
    entry["zenra"] = zenra
    entry["place"] = place

    posts.append(entry)

  return posts

def json2DF(jsonInput):
  df = pd.json_normalize(jsonInput)
  df_today = pd.DataFrame()

  df_csv = pd.read_csv(csvDir + "data.csv")
  
  df["naked_at"] = pd.to_datetime(df["naked_at"])
  df_csv["naked_at"] = pd.to_datetime(df_csv["naked_at"])
  df["created_at"] = pd.to_datetime(df["created_at"])
  df_csv["created_at"] = pd.to_datetime(df_csv["created_at"])

  df = df.astype({"article_id":"int64"})

  df2 = pd.concat([df, df_csv])
  df2 = df2.drop_duplicates(subset="created_at")
  
  df2 = df2.sort_values("article_id")
  df2.to_csv(csvDir + "data.csv", index=False)

  return df_csv["created_at"][0]

page1 = apiGet(1) #1ページ目取得
json1 = parse(page1[0])
cur = page1[1]
conv1 = json2DF(json1)

i = 1

while len(cur) != 0:
  i += 1
  time.sleep(3)
  page2 = apiGet(i, cur)
  cur = page2[1]
  json2 = parse(page2[0])
  conv2 = json2DF(json2)
