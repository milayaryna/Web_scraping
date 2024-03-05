# PTT 目標網站: https://www.ptt.cc/bbs/Gossiping/index.html

import pandas as pd
import requests
from bs4 import BeautifulSoup as BS

url = 'https://www.ptt.cc/bbs/Gossiping/index.html'
res =requests.get(url)
print(res.text) #直接使用套件會發現碰到18歲禁止進入

# 點入後，發現在網頁 index.html塞入一個over18=1的cookie requests套件即可
res = requests.get(url, cookies=dict(over18="1"), verify=False)
soup = BS(res.text)
title_list = [e.text for e in soup.select('.title')]
authour_list = [e.text for e in soup.select('.author')]
date_list = [e.text for e in soup.select('.date')]
push_list = [e.text for e in soup.select('.nrec')]
url_list = [e['href'] for e in soup.select('.title > a')]

df = pd.DataFrame()
df['push'] = push_list
df['date'] = date_list
df['title'] = title_list
df['author'] = authour_list
df['url'] = url_list
df
