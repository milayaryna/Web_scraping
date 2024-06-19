import requests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import io
	
def download_file():
    regex = re.compile(r'.*醫療機構.*\.ods')
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get('https://dep.mohw.gov.tw/DOMA/cp-4926-54415-106.html',headers = headers)
    soup = bs(res.text,'lxml')
    
    file_url = soup.find('section',{'class':'attachment'}).find('a',{'title':regex})['href']
    res = requests.get(file_url,headers = headers)
    df = pd.read_excel(res.content)
    df.to_csv(r'Data\醫療機構之基本資料.csv',index=False,encoding='utf_8_sig')
	
if __name__ == '__main__':
    print('Crawling Medical...')
    download_file()
    print('Done Crawling Medical！')
    print('='*85)
