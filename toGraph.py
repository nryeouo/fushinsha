import time
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
from mastodon import Mastodon

md = Mastodon(
  access_token = "(Your token)",
  api_base_url = "(Your server)"
)

df = pd.read_csv("data.csv")
today = dt.date.today()
thismonth = "%d 年 %d 月" % (today.year, today.month)

#上位7地域
df_top = df["place"].value_counts().head(7)

df_top.plot(kind = "barh", title="今月の全裸・下半身露出件数")
plt.savefig("a.png")

mediaid = md.media_post("a.png", "image/png")

time.sleep(2)

md.status_post(
  status = thismonth + " 1 日から現在までの全裸・下半身露出のうち、件数が多い 7 地域の状況は、以下の通りです。",
  media_ids = [mediaid["id"]],
  visibility = "Unlisted"
)
