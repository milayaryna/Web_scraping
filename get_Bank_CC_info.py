'''
目標網站:信用卡業務資訊揭露 https://www.banking.gov.tw/ch/home.jsp?id=192&parentpath=0,4&mcustomize=multimessage_view.jsp&dataserno=21207&aplistdn=ou=disclosure,ou=multisite,ou=chinese,ou=ap_root,o=fsc,c=tw&dtable=Disclosure

說明: 利用程式自動下載檔案，解壓縮後的excel檔經過簡單的ETL後存入資料庫。

預期目標:
1. 找到目標網站,例如:112年10月下載連結
2. 解壓縮檔案(.zip)
3. ETL
4. 存進資料庫
5. 封裝以上流程
'''

########## 1. 下載檔案 連結指向.zip檔案，只要使用request就能取得內容。先讓檔案存在本地端，再做後續處理。
import requests
url_download = 'http://www.fsc.gov.tw/fckdowndoc?file=/11210_信用卡重要資訊揭露.zip&flag=doc'
res = requests.get(url_download)

# 在想要的路徑底下建立壓縮檔名稱
zip_filename = 'C:/Users/122465/jupyter_notebook/爬蟲/credit_card_' + str(11210) + '.zip' 

# 將要爬的壓縮檔資料寫入剛剛建的檔案
with open(zip_filename,'wb') as handle:
    if not res.ok:
        print('OOPS, something wrong!')
    else:
        for block in res.iter_content(1024):
            handle.write(block)


########## 2. 解壓縮檔案(.zip)
# 建立解壓縮檔案名稱
zip_dir = 'C:/Users/122465/jupyter_notebook/爬蟲/credit_card_' + str(11210)
import zipfile
zip_ref = zipfile.ZipFile(zip_filename, 'r')
zip_ref.extractall(zip_dir)
zip_ref.close()


########## 3. ETL：處理下載的.xlsx檔案
import pandas as pd 
xlsx_path = zip_dir +'/'+ str(11210) + u'_信用卡重要資訊揭露.xlsx'
df = pd.read_excel(xlsx_path)

# 清理資料 觀察需要擷取的資料位在第8-44行 重新命名欄位 定義欄位資料型態(dtype)
df_temp = df[8:44];
col_names = [u'金融機構名稱',u'流通卡數',u'有效卡數',u'當月發卡數',u'當月停卡數',u'循環信用餘額(仟)',
             u'未到期分期付款餘額(仟)',u'當月簽帳金額(仟)',u'當月預借現金金額(仟)',
             u'逾期三個月以上比率(%)',
             u'逾期六個月以上比率(%)',
             u'備抵呆帳提足率(%)',u'當月轉銷呆帳金額(仟)',u'當年度轉銷呆帳金額(仟)'
]
df_temp.columns = col_names

df_part1 = df_temp[[u'流通卡數',u'有效卡數',u'當月發卡數',u'當月停卡數',u'循環信用餘額(仟)',
         u'未到期分期付款餘額(仟)',u'當月簽帳金額(仟)',u'當月預借現金金額(仟)',u'當年度轉銷呆帳金額(仟)']].astype('int64')
df_part2 = df_temp[[ u'逾期三個月以上比率(%)']].astype('float64') #才不會讓數字都變成0
df_temp['yyyymm'] = '201609'
df_result = pd.concat([df_temp[['yyyymm']],df_temp[u'金融機構名稱'],df_part1,df_part2],axis=1)

# 給予欄位正確SQL資料型態
from sqlalchemy.dialects.mssql import VARCHAR,NVARCHAR,FLOAT,INTEGER 
dict_dtype={
        u'yyyymm':NVARCHAR(length=6),
        u'金融機構名稱':NVARCHAR(length=20),
        u'流通卡數':INTEGER,
        u'有效卡數':INTEGER,
        u'當月發卡數':INTEGER,
        u'當月停卡數':INTEGER,
        u'循環信用餘額(仟)':INTEGER,
        u'未到期分期付款餘額(仟)':INTEGER,
        u'當月簽帳金額(仟)':INTEGER,
        u'當月預借現金金額(仟)':INTEGER,
        u'逾期三個月以上比率(%)':FLOAT,
        u'當月轉銷呆帳金額(仟)':INTEGER
}

# 連結MS資料庫 寫入資料
import pyodbc 
import sqlalchemy 
connect_w = lambda x:pyodbc.connect('DRIVER={SQL Server};SERVER=dbm_public;DATABASE=External;PWD=01060728;CHARSET=utf8;',
                                     unicode_result=True)
conn_w = sqlalchemy.create_engine('mssql://',creator=connect_w).connect()


########## 4.連結MS資料庫 寫入資料
import pyodbc 
import sqlalchemy 
connect_w = lambda x:pyodbc.connect('DRIVER={SQL Server};SERVER=dbm_public;DATABASE=External;PWD=01060728;CHARSET=utf8;',
                                     unicode_result=True)
conn_w = sqlalchemy.create_engine('mssql://',creator=connect_w).connect()

df_result.to_sql(u'測試table',conn_w,index = False, if_exists = 'replace',dtype=dict_dtype); # 若要資料要不斷累加，if_exists = 'append'


########## 5.包裝成函數
import requests
import zipfile
import pandas
import pyodbc,sqlalchemy
from sqlalchemy.dialects.mssql import VARCHAR,NVARCHAR,FLOAT,INTEGER 



def getBankCCinfo(yyymm):
    ''' 下載並解壓縮，信用卡重要業務及財務資訊揭露.zip
    參數:
        民國年月- 10509    
        
    '''
    assert isinstance(yyymm,int),'PLEASE Enter INT tyep such as 11210' # 確認輸入為整數
    url_download = 'http://www.fsc.gov.tw/fckdowndoc?file=/{}_信用卡重要資訊揭露.zip&flag=doc'.format(yyymm)
    res = requests.get(url_download)
    
    zip_dir = './data/credit_card_' + str(yyymm)
    zip_filename = './data/credit_card_' + str(yyymm) + '.zip' 
    
    ### 存檔
    with open(zip_filename,'wb') as handle:
        if not res.ok:
            print 'OOPS, something wrong!'
        else:
            for block in res.iter_content(1024):
                handle.write(block)
    
    ### 解壓縮    
    zip_ref = zipfile.ZipFile(zip_filename, 'r')
    zip_ref.extractall(zip_dir)
    zip_ref.close()

    
def parseXLSXtoSQL(yyymm):
    
    assert isinstance(yyymm,int) #確認輸入為整數
    
    zip_dir = './data/credit_card_' + str(yyymm)
    xlsx_path = zip_dir +'/'+ str(yyymm) + u'_信用卡重要資訊揭露.xlsx'
    df = pd.read_excel(xlsx_path)
    df_temp = df[8:44];
    col_names = [u'金融機構名稱',u'流通卡數',u'有效卡數',u'當月發卡數',u'當月停卡數',u'循環信用餘額(仟)',
                 u'未到期分期付款餘額(仟)',u'當月簽帳金額(仟)',u'當月預借現金金額(仟)',
                 u'逾期三個月以上比率(%)',
                 u'逾期六個月以上比率(%)',
                 u'備抵呆帳提足率(%)',u'當月轉銷呆帳金額(仟)',u'當年度轉銷呆帳金額(仟)'
    ]
    df_temp.columns = col_names
    
    df_part1 = df_temp[[u'流通卡數',u'有效卡數',u'當月發卡數',u'當月停卡數',u'循環信用餘額(仟)',
         u'未到期分期付款餘額(仟)',u'當月簽帳金額(仟)',u'當月預借現金金額(仟)',u'當年度轉銷呆帳金額(仟)']].astype('int64')
    df_part2 = df_temp[[ u'逾期三個月以上比率(%)']].astype('float64')
    
    
    yyyy = 1911 + int(str(yyymm)[:3]) # 112 --> 2023
    yyyymm = str(yyyy) + str(yyymm)[-2:]
    
    
    df_temp['yyyymm'] = yyyymm
    df_result = pd.concat([df_temp[['yyyymm']],df_temp[u'金融機構名稱'],df_part1,df_part2],axis=1)
    dict_dtype={
        u'yyyymm':NVARCHAR(length=6),
        u'金融機構名稱':NVARCHAR(length=20),
        u'流通卡數':INTEGER,
        u'有效卡數':INTEGER,
        u'當月發卡數':INTEGER,
        u'當月停卡數':INTEGER,
        u'循環信用餘額(仟)':INTEGER,
        u'未到期分期付款餘額(仟)':INTEGER,
        u'當月簽帳金額(仟)':INTEGER,
        u'當月預借現金金額(仟)':INTEGER,
        u'逾期三個月以上比率(%)':FLOAT,
        u'當月轉銷呆帳金額(仟)':INTEGER
    }
    
    df_result.to_sql(u'測試table',conn_w,index = False, if_exists = 'replace',dtype=dict_dtype)
    return df_result


if __name__ == '__main__':

    connect_w = lambda x:pyodbc.connect('DRIVER={SQL Server};SERVER=dbm_public;DATABASE=External;PWD=01060728;CHARSET=utf8;',
                                     unicode_result=True)
    conn_w = sqlalchemy.create_engine('mssql://',creator=connect_w).connect()
    
    getBankCCinfo(11210)
    parseXLSXtoSQL(11210)
    
    conn_w.close()
