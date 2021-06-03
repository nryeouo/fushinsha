import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import textgraph

cred = credentials.Certificate(
    "[path-to-file]")
firebase_admin.initialize_app(cred)

JST = dt.timezone(dt.timedelta(hours=+9), 'JST')

today = dt.datetime.now().replace(
    hour=0, minute=0, second=0, microsecond=0,
    tzinfo=JST)

firstDay = dt.datetime.now().replace(
    day=1, hour=0, minute=0, second=0, microsecond=0,
    tzinfo=JST)

today_ja = today.strftime("%Y 年 %m 月 %d 日")
time_ja = dt.datetime.now().strftime("%H 時 %M 分")


def getArticles(day):
    db = firestore.client()
    dayBefore = (today - relativedelta(days=day))
    docu = db.collection("article_id").where("created_at", ">=", dayBefore)
    list1 = []

    for article in docu.stream():
        dict1 = {}

        for k, v in article.to_dict().items():
            dict1[k] = v

        list1.append(dict1)

    df = pd.DataFrame(list1)

    # UTC -> JST
    df = df.set_index("naked_at")
    df.index = df.index.tz_convert(JST)
    df = df.reset_index()
    df = df.set_index("created_at")
    df.index = df.index.tz_convert(JST)
    return df


def cutDataFrame(data, day):
    todayData = pd.DataFrame()
    todayData = todayData.append(
        data[data.index.date >= (dt.date.today() - relativedelta(days=day))])
    return todayData


def totalCases(data):
    cases = data["zenra"].value_counts()
    kahan = 0
    zenra = 0

    if "下半身露出" in cases:
        kahan = cases["下半身露出"]
    if "全裸" in cases:
        zenra = cases["全裸"]

    return kahan, zenra


def todofuken(data):
    zenraplace = data["place"].value_counts()

    zp2 = zenraplace.reset_index(name="count")

    pref = ["北海道",
            "青森", "岩手", "宮城", "秋田", "山形", "福島",
            "茨城", "栃木", "群馬", "埼玉", "千葉", "東京", "神奈川",
            "新潟", "富山", "石川", "福井",
            "山梨", "長野", "岐阜", "静岡", "愛知",
            "三重", "滋賀", "京都", "大阪", "兵庫", "奈良", "和歌山",
            "鳥取", "島根", "岡山", "広島", "山口",
            "徳島", "香川", "愛媛", "高知",
            "福岡", "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄"]

    zp2["prefcode"] = zp2["index"].apply(lambda x: pref.index(x))
    zp2 = zp2.sort_values(by=["count", "prefcode"], ascending=[False, True])
    zp2 = zp2.set_index("index")

    zp3 = zp2["count"]

    if len(zp3) == 0:
        return ""

    placedict = zp3.to_dict()
    tdfkmsg = "（"

    for place, times in placedict.items():
        tdfkmsg += f"{place}: {times}、"

    tdfkmsg = tdfkmsg.rstrip("、") + "）\n"

    return tdfkmsg


def calcAverage(data):
    daily = data.groupby(data.index.date).count()["zenra"]
    average = daily.mean()
    return average


recentCases = getArticles(7 if today.day < 7 else today.day)
dfToday = cutDataFrame(recentCases, 0)
dfWeek = cutDataFrame(recentCases, 7)


def top3(data):
    top = data["place"].value_counts()[:3]
    total = data["place"].value_counts().sum()
    top.index = top.index.str.rjust(3, "　")
    topList = list(zip(top.index, top))
    graph = textgraph.horizontal(topList, width=10).splitlines()
    for i in range(len(graph)):
        graph[i] += f"　{top.values[i]}"
    graph = f"《今月の合計》 {total} 件\n" + "\n".join(graph)
    return graph


def composeMessage():
    message = ""
    message += f"{today_ja} {time_ja}までに発表された\
下半身の露出は {totalCases(dfToday)[0]} 件、\
全裸は {totalCases(dfToday)[1]} 件でした。\n"

    message += todofuken(dfToday)
    message += f"発表件数の 7 日平均値は {calcAverage(dfWeek):.2f} 件です。\n\n"
    message += top3(cutDataFrame(recentCases, today.day))

    return message
