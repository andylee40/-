import urllib.request #urllib2.urlopen 
import zipfile #zipfile.ZipFile
import pandas as pd
import mysql.connector as connector
import csv
import os
import re
import numpy as np
import pymysql
import glob
import datetime as yester
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer,Date
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

#開始爬蟲
def DownloadTWCompany():
    # 檔案下載
    print("下載全國營業(稅籍)登記資料集壓縮檔...")
    downloadurl = urllib.request.urlopen('https://eip.fia.gov.tw/data/BGMOPEN1.zip')
    zipcontent= downloadurl.read()
    with open("TWRAW.zip", 'wb') as f:
        f.write(zipcontent)
    print ("下載完成!")
    #解壓縮檔案
    print ("資料解壓縮...")
    with zipfile.ZipFile(open('TWRAW.zip', 'rb')) as f:
        f.extractall()
    print ("解壓縮完成!")
    
#檔案重新命名與移動
def Rename():
    file_oldname = os.path.join("/var/www/html/hong_pet/財政部稅籍", "TWRAW.zip")
    file_newname_newfile = os.path.join("/var/www/html/hong_pet/財政部稅籍/tax_day_v2", 'tax_'+(datetime.now().strftime("%Y%m%d"))+'.zip')
    file_oldname1 = os.path.join("/var/www/html/hong_pet/財政部稅籍", "BGMOPEN1.csv")
    file_newname_newfile1 = os.path.join("/var/www/html/hong_pet/財政部稅籍/tax_day_v2", 'tax_'+(datetime.now().strftime("%Y%m%d"))+'.csv')
    os.rename(file_oldname, file_newname_newfile)
    os.rename(file_oldname1, file_newname_newfile1)
    print ("檔案移動完成")
    
def Reset(df):
    pattern = re.compile(r'\s+')
    res = re.compile("[^\\u4e00-\\u9fa5^a-z^A-Z^0-9]")
    re_str = r'..[縣市]'
    
    df=df.iloc[1:,:].reset_index(drop=True)
    df=df.applymap(lambda x: pattern.sub('', x) if isinstance(x, str) else x)
    df=df.applymap(lambda x: res.sub('', x) if isinstance(x, str) else x)
    df=df[df['名稱'].str.contains('寵物')|df['名稱1'].str.contains('寵物')|df['名稱2'].str.contains('寵物')|df['名稱3'].str.contains('寵物')]
    df=df.reset_index(drop=True)

    df=df.copy()
    df['縣市']=df['營業地址'].apply(lambda x:re.findall(re_str,x)[0])
    df['類別']=''
    df['新增時間']=''
    df['移除時間']=''
    for i in range(0,len(df)):
        if df.iloc[i,9].find("寵物") != -1:
            df.iloc[i,17]=df.iloc[i,9]
        elif df.iloc[i,11].find("寵物") != -1:
            df.iloc[i,17]=df.iloc[i,11]
        elif df.iloc[i,13].find("寵物") != -1:
            df.iloc[i,17]=df.iloc[i,13]
        else:
            df.iloc[i,17]=df.iloc[i,15]
    df=df[['縣市','營業人名稱','統一編號','類別','新增時間','移除時間']]
    return df

def Different():
    global change,df2_1,table
    
    #獲得今天的日期
    today = yester.date.today() 
    #獲得昨天的日期
    yesterday = today - yester.timedelta(days=1)
    
    #昨日資料
    oldfile=sorted(glob.glob('tax_day_v2/*.csv'), key=os.path.getmtime)[-2]
    df1=pd.read_csv(oldfile,dtype='object',keep_default_na=False)
    
    #今日資料
    df2=pd.read_csv('tax_day_v2/tax_'+(datetime.now().strftime("%Y%m%d"))+'.csv',dtype='object',keep_default_na=False)
    
    
    df1_1=Reset(df1)
    df2_1=Reset(df2)
    
    df1_1=df1_1.replace('其他殯葬及寵物生命紀念相關服務','寵物殯葬')
    df2_1=df2_1.replace('其他殯葬及寵物生命紀念相關服務','寵物殯葬')
    
    #2天相異資料紀錄
#     df3= df1_1.append(df2_1)
    df3=pd.concat([df1_1,df2_1],ignore_index=True).reset_index(drop=True)
    diffrent = df3.drop_duplicates(subset=['營業人名稱','類別'],keep=False)
    check=diffrent['營業人名稱'].values
    
    #刪減資料集
    delete=df1_1[df1_1["營業人名稱"].isin(check)]
    delete = delete[delete['營業人名稱'].notnull()].copy()
    delete['移除時間'] = delete['移除時間'].replace('',datetime.now().strftime("%Y-%m-%d"))
    
    #增加資料集
    insert=df2_1[df2_1["營業人名稱"].isin(check)]
    insert = insert[insert['營業人名稱'].notnull()].copy()
    insert['新增時間'] = insert['新增時間'].replace('',(datetime.now().strftime("%Y-%m-%d")))
    
    #異動資料數量
#     change=delete.append(insert)
    change=pd.concat([delete,insert],ignore_index=True).reset_index(drop=True)
    
    
     #另一個資料庫的資料
    dftable=df2_1['類別'].value_counts(dropna=False).reset_index()
#     #整理成每日資料
#     dftable=dftable.rename(columns={"index":"日期","類別":"number"})
    #data_dict = df2.to_dict(orient='records')
    dftable['日期']=datetime.now().strftime("%Y%m%d")
    table = dftable.pivot_table(values='count', index='日期', columns='類別')
    table=table[['寵物批發','寵物服裝及其飾品配件批發','寵物殯葬','寵物照顧及訓練','寵物用品批發','寵物用品零售','寵物零售','寵物飼品零售']].reset_index()
    print('資料處理完成')
    
def Insert():
    hostname=""
    dbname=""
    uname=""
    pwd=""
    #Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8".format(host=hostname, db=dbname, user=uname, pw=pwd))
    
    try:
        df2_1.to_sql('tax_sep', engine, index=False,if_exists='replace')
        print('資料插入tax_sep成功')
    except Exception as e:
        print(e)
    
    try:
        table.to_sql('tax', engine, index=False,if_exists='append')
        print('資料插入tax成功')
    except Exception as e:
        print(e)
    
    try:
        change.to_sql('tax_different', engine, index=False,if_exists='append')
        print('資料插入tax_different成功')
    except Exception as e:
        print(e)
        
#刪除csv
def Removecsv():
    today2 = yester.date.today() #獲得今天的日期
    yesterday2 = today2 - yester.timedelta(days=2)
    #oldfile2='tax_day/tax_'+yesterday2.strftime("%Y%m%d")+'_1.csv'
    #資料按修改日期排序，倒數第4筆即為前天資料
    oldfile2=sorted(glob.glob('tax_day_v2/*.csv'), key=os.path.getmtime)[-3]
    #fileTest ='tax_day_v2/tax_'+(datetime.now().strftime("%Y%m%d"))+'.csv'
    try:
        os.remove(oldfile2)
        #os.remove(fileTest)
    except OSError as e:
        print(e)
    else:
        print("檔案刪除成功")
        
#執行
if __name__ == '__main__':
    DownloadTWCompany()
    Rename()
    Different()
    Insert()
    Removecsv()