import requests
from bs4 import BeautifulSoup as bs
import os
import re
from send2trash import send2trash
import datetime

'''
財政部實價登錄網站將資料分成三區，
1.季資料區
2.期資料區
3.最新一期資料
隨時間經過，最新一期資料將移至「期資料區」，過了一季後，
所有在「期資料區」的資料將合併成一個檔案新增至「季資料區」。

當初目標是取得過去兩年的平均房價，
因此爬蟲規劃為，
每次下載最新8季的季資料，以及全部「期資料區」的期資料，以及最新一期資料
'''

# 下載最新期數
def download_latest_period():
    print('  Downloading latest period...')
    
    res = requests.get('https://plvr.land.moi.gov.tw/Download_ajax_active',headers={'User-Agent':'Mozilla/5.0'})
    soup = bs(res.text,'lxml')
    file_date_regex = re.compile(r'資料內容：登記日期 (\d+)年(\d+)月(\d+)日至 (\d+)年(\d+)月(\d+)日之買賣案件')
    description_text = soup.find("span", {"class": "text-danger"}).text
    print(description_text)
    search_res = file_date_regex.search(description_text)
    latest_date = datetime.datetime(int(search_res.group(4))+1911,int(search_res.group(5)),int(search_res.group(6)))
    latest_date_str = datetime.datetime.strftime(latest_date,'%Y%m%d')
    
    if not os.path.exists(r'Data\periods'):
        os.makedirs(r'Data\periods')
    if not '{}.zip'.format(latest_date_str) in os.listdir(r'Data\periods'):
        print('     Downloading New House Price file of period： {}.zip'.format(latest_date_str))
        res_latest = requests.get('https://plvr.land.moi.gov.tw//Download?type=zip&fileName=lvr_landcsv.zip',headers={'User-Agent':'Mozilla/5.0'})
        with open('Data\periods\{}.zip'.format(latest_date_str),'wb') as f:
            f.write(res_latest.content)
    return latest_date_str
        
# 下載所有期數
def download_periods():
    print('  Downloading all periods...')
    if not os.path.exists(r'Data\periods'):
        os.makedirs(r'Data\periods')
    url_period = 'https://plvr.land.moi.gov.tw/DownloadHistory_ajax_list'
    res_period = requests.get(url_period,headers={'User-Agent': 'Mozilla/5.0'})
    soup_period = bs(res_period.text,'lxml')

    reg = re.compile(r'發布日期 (\d\d\d\d\d\d\d\d)')
    file_date_regex = re.compile(r'資料內容：登記日期 (\d+)年(\d+)月(\d+)日至 (\d+)年(\d+)月(\d+)日之買賣案件')
    
    all_periods_dic = {}
    for item in [a for a in soup_period.find_all('td') if '發布日期' in a.text]:
        public_date = reg.search(item.text).group(1)
        data_desc = file_date_regex.search(item.find('span')['desc']).group()
        data_desc_search = file_date_regex.search(data_desc)
        data_to_date = datetime.datetime(int(data_desc_search.group(4))+1911,int(data_desc_search.group(5)),int(data_desc_search.group(6)))
        data_to_date_str = datetime.datetime.strftime(data_to_date,'%Y%m%d')
        all_periods_dic[public_date] = data_to_date_str
    if len(all_periods_dic)>0:
        for public,to_date in all_periods_dic.items():
            download_filename = '{}.zip'.format(to_date)
            if not  download_filename in os.listdir(r'Data\periods'):
                print('     Downloading New House Price file of period： {}'.format(download_filename))
                res = requests.get('https://plvr.land.moi.gov.tw/DownloadHistory?type=history&fileName={}'.format(public),headers={'User-Agent': 'Mozilla/5.0'})
                with open(r'Data\periods\{}'.format(download_filename),'wb') as f:
                    f.write(res.content)
            
    latest_date_str = download_latest_period()
    all_periods_dic['latest_period'] = latest_date_str
    
    for f in os.listdir('Data\periods'):
        if not f.replace('.zip','') in all_periods_dic.values():
            send2trash(r'Data\periods\{}'.format(f))
            print('     send2trash {}'.format(f))
            
# 下載過去8季
def download_seasons():
    print('  Downloading past 8 seasons...')
    if not os.path.exists(r'Data\seasons'):
        os.makedirs(r'Data\seasons')
    url_season = 'https://plvr.land.moi.gov.tw/DownloadSeason_ajax_list'
    res_season = requests.get(url_season,headers={'User-Agent': 'Mozilla/5.0'})
    soup_season = bs(res_season.text,'lxml')
    latest_seasons = []
    
    for season_tag in soup_season.find('select',{'id':'historySeason_id'}).find_all('option'):
        if len(latest_seasons) == 8:
            break
        season = season_tag.text
        season_filename = season+'.zip'
        if not season_filename in os.listdir('Data\seasons'):
            print('     Downloading New House Price file of season： {}'.format(season_filename))
            season_url = 'https://plvr.land.moi.gov.tw//DownloadSeason?season={}&type=zip&fileName=lvr_landcsv.zip'.format(season_tag['value'])
            res = requests.get(season_url,headers={'User-Agent': 'Mozilla/5.0'})
            with open(os.path.join('Data\seasons',season_filename),'wb') as f:
                f.write(res.content)
        else:
            pass
        latest_seasons.append(season_filename)
        
    for f in os.listdir('Data\seasons'):
        if not f in latest_seasons:
            send2trash(r'Data\seasons\{}'.format(f))
            print('     send2trash {}'.format(f))
    return latest_seasons
    
if __name__ == '__main__':
    print('Crawling House Price...')
    download_seasons()
    download_periods()
    print('Done Crawling House Price！')
    print('='*85)
