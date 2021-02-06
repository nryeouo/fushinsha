import datetime as dt
import pandas as pd
from mastodon import Mastodon
import re
import requests

zenhan = str.maketrans("１２３４５６７８９０","1234567890","")

md = Mastodon(
  access_token = "userID.dat",
  api_base_url = "https://machida.yokohama"
)

token = "***CENSORED***"
query = "unit_id:133089874031904245 全裸 OR 下半身"
limit = "50"

auth = {"Authorization": token}
parameter = {"query": query, "limit": limit}
url = "https://api.nordot.jp/v1.0/search/contentsholder/posts.list"

r = requests.get(url, params = parameter, headers = auth)
j = r.json()

today = dt.date.today()

def parse():
  i = 0
  posts = []

  for post in j["posts"]:

    entry = {}
    title  = post["title"].translate(zenhan)
    desc   = post["description"].translate(zenhan)
    pub_at = dt.datetime.strptime(post["published_at"], "%Y-%m-%dT%H:%M:%S+00:00")
    pub_at += dt.timedelta(hours=9) #Japan

    # 露出時刻
    if "分ごろ" not in desc:
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
    entry["naked_at"] = dt.datetime(
      ndate.year, ndate.month, ndate.day,
      ntime.hour, ntime.minute, ntime.second
    )
    entry["created_at"] = pub_at
    entry["zenra"] = zenra
    entry["place"] = place

    posts.append(entry)
    i += 1

  return posts

def json2DF(jsonInput):
  df = pd.json_normalize(jsonInput)
  df_today = pd.DataFrame()

  df["naked_at"] = pd.to_datetime(df["naked_at"])
  df["created_at"] = pd.to_datetime(df["created_at"])

  df_today = df_today.append(df[df["created_at"] >= pd.Timestamp(today)]) # 本日分

  return df_today

def totalCases(todayD):
  cases = todayD["zenra"].value_counts()
  kahan = 0
  zenra = 0

  if "下半身露出" in cases:
    kahan = cases["下半身露出"]
  if "全裸" in cases:
    zenra = cases["全裸"]
  
  kahan_zenra = "下半身の露出は %d 件、全裸は %d 件でした。" % (kahan, zenra)
  return kahan_zenra

def todofuken(todayD):
  zenraplace = todayD["place"].value_counts()
  placedict = zenraplace.to_dict()
  tdfkmsg = "地域別の件数は、"

  for place, times in placedict.items():
    tdfkmsg += ("%sで %d 件、") % (place, times)
  
  tdfkmsg += "でした。"

  return tdfkmsg


jsonData = parse()

readJ = json2DF(jsonData)
kensu = totalCases(readJ)
basho = todofuken(readJ)

today_ja = today.strftime("%Y 年 %m 月 %d 日")
time_ja  = dt.datetime.now().strftime("%H 時 %M 分")

message = "%s %sまでに発表された%s\n%s" % (today_ja, time_ja, kensu, basho)

print(message)

'''
md.status_post(
  status = message,
  visibility = "Unlisted"
)
'''
