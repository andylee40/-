from selenium import webdriver
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC    
import pandas as pd
import numpy as np
import time
import os
import pymysql
import glob
from datetime import datetime
import datetime as yester
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer,Date
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

def Scrapy():
    global df
    #儲存資料
    data=[]
    #從首頁開始
    page=1
    #防止跳出視窗
    options = Options()
    options.add_argument("--disable-notifications")
    options.headless = True
    
    url="https://www.pet.gov.tw/Web/BusinessList.aspx?PG={}".format(page)
    driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"),options=options)
    driver.get(url)
    driver.find_elements(By.XPATH,"//*[@id='js-scroll-here']/div[2]/a[1]")[0].click()
    pages=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME,"tableCount")))
    maxpage=int(''.join([x for x in pages.text.split('/')[1] if x.isdigit()]))
    #開始爬蟲
    while True:
        print("目前頁數：",page)
        table_data = driver.find_elements(By.XPATH,"//table[@id]/tbody/tr")
        for row in table_data:
            columns = row.find_elements(By.XPATH,"./td") # Use dot in the xpath to find elements with in element.
            table_row = []
            for column in columns:
                table_row.append(column.text)
            #print(table_row)
            data.append(table_row)

        if page < maxpage:
            try:
                page+=1
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
                time.sleep(5)
                driver.find_elements(By.XPATH,"//*[@id='enrf-pagination']/ul/li[10]/a")[0].click()
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,"//table[@id]/tbody/tr")))
            except:
                pass
        else:
            break
    driver.close()
    print('工作完成')
    #匯出檔案
    df=pd.DataFrame(data=data,columns=['所屬縣市','特定寵物業許可證號','營業場所名稱','經營業務項目','特定寵物種類','評鑑等級','營業狀態','有效日期'])

def Clear():
    #new
    df2=df.copy()
    df2.drop_duplicates(keep='first', inplace=True)
    print("爬蟲獲取資料共:"+str(len(df2))+"筆")
    df2['新增時間']=''
    df2['移除時間']=''
    
    #old
    today = yester.date.today() #獲得今天的日期
    yesterday = today - yester.timedelta(days=1)
    #oldfile='manage_day/manage_'+yesterday.strftime("%Y%m%d")+'_1.csv'
    oldfile=sorted(glob.glob('manage_day/*.csv'), key=os.path.getmtime)[-1]
    #昨日資料
    dfold=pd.read_csv(oldfile,dtype='object',keep_default_na=False)
    dfold1=dfold[dfold['移除時間']==''].reset_index(drop=True)
    
#     df3= dfold1.append(df2)
    df3=pd.concat([dfold1,df2],ignore_index=True).reset_index(drop=True)
    diffrent = df3.drop_duplicates(subset=['所屬縣市', '特定寵物業許可證號', '營業場所名稱', '經營業務項目', '特定寵物種類', '評鑑等級', '營業狀態','有效日期'],keep=False)
    
    check=diffrent['營業場所名稱'].values
    
    #刪減資料集
    delete=dfold1[dfold1["營業場所名稱"].isin(check)]
    delete = delete[delete['移除時間'].notnull()].copy()
    delete['移除時間'] = delete['移除時間'].replace('',datetime.now().strftime("%Y%m%d"))
    
    #增加資料集
    insert=df2[df2["營業場所名稱"].isin(check)]
    insert = insert[insert['新增時間'].notnull()].copy()
    insert['新增時間'] = insert['新增時間'].replace('',(datetime.now().strftime("%Y%m%d")))
    
    
#     change=delete.append(insert)
    change=pd.concat([delete,insert],ignore_index=True).reset_index(drop=True)
    checkold=change['營業場所名稱'].values
    
    #從舊資料刪除刪減資料集
    #print(np.where(df["營業場所名稱"].isin(check)))
    dfold=dfold.drop(dfold.index[np.where(dfold["營業場所名稱"].isin(checkold))])
#     dfold=dfold.append(change).reset_index(drop=True)
    dfold=pd.concat([dfold,change],ignore_index=True).reset_index(drop=True)
    print("異動資料共:"+str(len(change))+"筆")
    print("最新資料共:"+str(len(dfold[dfold['移除時間']=='']))+"筆")
    
    #匯出最新資料
    dfold.to_csv('manage_day/manage_'+(datetime.now().strftime("%Y%m%d"))+'_1.csv',index=False)
    
def Insert():
    newst='/var/www/html/hong_pet/寵物業管理系統/manage_day/manage_'+(datetime.now().strftime("%Y%m%d"))+'_1.csv'
    #連線資料庫
    connection= pymysql.connect(user='',password='',host='',database='',local_infile=1)
    #清空資料庫
    cursor = connection.cursor()
    query1="TRUNCATE TABLE pet_manage"
    cursor.execute(query1)
    connection.commit()
    #插入最新資料
    query2="LOAD DATA LOCAL INFILE '" +newst+ """' INTO TABLE pet_manage CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    IGNORE 1 ROWS;"""
    cursor.execute(query2)
    connection.commit()
    #修改錯誤日期
    query3="UPDATE pet_manage SET 新增時間 = NULL WHERE 新增時間  = 0000-00-00;"
    cursor.execute(query３)
    connection.commit()
    #修改錯誤日期
    query4="UPDATE pet_manage SET 移除時間 = NULL WHERE 移除時間  = 0000-00-00;"
    cursor.execute(query4)
    connection.commit()
    #關閉游標
    cursor.close()
    #關閉資料庫連線
    connection.close()

def Insert2():
    hostname=""
    dbname=""
    uname=""
    pwd=""
    #Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8".format(host=hostname, db=dbname, user=uname, pw=pwd))
    df_day=df.drop_duplicates(keep='first')
    try:
        df_day.to_sql('pet_manage2', engine, index=False,if_exists='replace')
        print('資料插入pet_manage2成功')
    except Exception as e:
        print(e)    
    
    
def Zip():
    df.to_csv('manage_day/manage_'+(datetime.now().strftime("%Y%m%d"))+'.zip', compression={'method': 'zip', 'archive_name': 'manage_'+
                                                                                            (datetime.now().strftime("%Y%m%d"))+'.csv'},index=False)
#刪除csv
def Removecsv():
    today2 = yester.date.today() #獲得今天的日期
    yesterday2 = today2 - yester.timedelta(days=2)
    oldfile2='manage_day/manage_'+yesterday2.strftime("%Y%m%d")+'_1.csv'
    try:
        os.remove(oldfile2)
    except OSError as e:
        print(e)
    else:
        print("檔案刪除成功")
    
    
if __name__ == '__main__':
    Scrapy()
    Zip()
    Clear()
    Insert()
    Insert2()
    Removecsv()
    