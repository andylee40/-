from bs4 import BeautifulSoup
from datetime import datetime
import os
import pymysql
import datetime as yester
import requests
import urllib.parse
import pandas as pd 
import numpy as np
import time
import math
import re
import json
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer,Date
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base



def Scrapy():
    global df
    headers = {
        'authority': 'petfood.moa.gov.tw',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': '_ga=GA1.1.713818175.1693465761; ASP.NET_SessionId=t4hofkg01o1xf240in4chgib; _ga_GGFKGXEJ2S=GS1.1.1693810318.1.0.1693810323.0.0.0; _ga_WDM95BBVT0=GS1.1.1693884944.1.1.1693885389.0.0.0',
        'origin': 'https://petfood.moa.gov.tw',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    
    source_dict={1:'製造、加工',2:'委託代工廠製造',3:'輸入',4:'分裝'}
    category_dict={1:'乾飼糧',2:'半溼性飼糧(水分含量20-35％)',3:'罐頭',4:'生鮮、冷凍',5:'零食',6:'潔牙骨',7:'補助食品'}
    
    url='https://petfood.moa.gov.tw/Handler/Web/FoodHandler.ashx'
    
    #儲存資料
    colist=[]

    #從首頁開始
    page=1
    pagesize=200
    error_data=0
    
    #開始爬蟲
    while True:
        try:
            time.sleep(1)
            json_data = {
                    'Method': 'FoodData',
                    'Param': {
                        'action': 'getFoodDataList',
                        'FoodStatus': '9',
                        'CompanyID': '',
                        'currentPage': page,
                        'pageSize': pagesize,
                        'orderByColumn': 'UpdateDate',
                        'sortDirection': 'desc',
                        'OpenFlag': '',
                        'SourceType': '',
                        'ItemType': '',
                        'CompanyName': '',
                        'Compilation': '',
                        'Suitable': '',
                        'KeyWord': '',
                    },
                }
            response = requests.post(
                                    'https://petfood.moa.gov.tw/Handler/Web/FoodHandler.ashx',
                                    headers=headers,
                                    json=json_data,
                                    )
            soup = json.loads(response.text)
            maxpage=math.ceil(int(soup['totalSize'])/pagesize)
            foodid=[(item['FoodID'],item['CompanyTypeName']) for item in json.loads(soup['Message'])]
            
            for ids in foodid:
                
#                 time.sleep(1)
                idd=ids[0]
                typez=ids[1]
                json_data2 = {
                                'Method': 'FoodData',
                                'Param': {
                                    'action': 'getFoodDataDetail',
                                    'FoodID': idd,
                                },
                            }

                response2 = requests.post(
                    'https://petfood.moa.gov.tw/Handler/Web/FoodHandler.ashx',
                    headers=headers,
                    json=json_data2,
                )
                
                fooddata=json.loads(response2.text)
                food_infor=json.loads(fooddata['Message'])
                try:
                    detail=food_infor['_FoodDetail'][0]
                    
                    try:
                        #狀態
                        status=detail['OpenFlag']
                    except:
                        status=None
                    
                    try:
                        #標題
                        title=detail['FoodName']
                    except:
                        title=None
                    
                    try:
                        #業者名稱
                        company=detail['CompanyName']
                    except:
                        company=None
                    
                    try:
                        #產品來源
                        source=detail['SourceType']
                    except:
                        source=None
                        
                    try:
                        #產品種類
                        category=detail['ItemType']
                    except:
                        category=None
                        
                    try:
                        #重量容量錠數
                        heavy=detail['UnitQty']+'+'+detail['UnitQty_SEM']+detail['UnitType']
                    except:
                        heavy=None
                    
                    try:
                        #主要原料
                        main_ingre=detail['Material']
                    except:
                        main_ingre=None
                    #主要營養成分
                    try:
                        main_nutrition=detail['Nutrient']
                        if main_nutrition=='+++串新版格式+++':
                            main_nutrition=''
                            for i in food_infor['_Nutrient']:
                                main_nutrition+=i['NutrientName']+':'+i['NutrientQty']+i['NutrientUnit']+','
                    except:
                        main_nutrition=None
                        pass
                    #適用寵物種類
                    forwho=detail['Suitable']+','+detail['Instructions']+detail['Preservation']+'。'
                    #加工業名稱
                    try:
                        detail3=food_infor['_Oems'][0]
                        make=detail3['OemName']
                    except:
                        make=None
                        pass
                        

                    colist.append({'狀態':status,
                                    '標題':title,
                                    '業者名稱':typez+','+company,
                                    '產品來源':int(source),
                                    '產品種類':int(category),
                                    '重量容量錠數':heavy,
                                    '主要原料及添加物':main_ingre,
                                    '主要營養成分及含量': main_nutrition,
                                    '適用寵物種類及使用方法及保存方法':forwho ,
                                    '製造或加工業者工廠名稱':make,
                                    '產品外包裝照片':None})
                except:
                    error_data+=1
                    print('錯誤筆數累積:{}'.format(error_data))
                    pass
            if page < maxpage:
                print("目前完成頁數為 {0} , 剩下 {1} 頁未完成".format(page,maxpage-page))
                page+=1
            else:
                print("目前完成頁數為 {} , 這是最後一頁".format(page))
                print('-'*50)
                print("工作完成")
                break
        except Exception as e:
            print("錯誤頁數:",page)
            print(e)
            break
    #匯出檔案
    df = pd.DataFrame(colist)
    df.replace("\r","", regex=True, inplace=True)
    df.replace("\n","", regex=True, inplace=True)
    df.replace("\r\n","", regex=True, inplace=True)
    df.replace("\t","", regex=True, inplace=True)
    df['產品來源'] = df['產品來源'].map(source_dict)
    df['產品種類'] = df['產品種類'].map(category_dict)
    df.to_csv('food_daily/petfood_'+(datetime.now().strftime("%Y%m%d"))+'.csv',index=False)
    return df


def Clear(pet_food_different):
    
    #今日資料
    df_new=pd.read_csv('food_daily/petfood_'+yester.date.today().strftime("%Y%m%d")+'.csv',dtype='object',keep_default_na=False)
    pattern = re.compile(r'\s+')
    df_new=df_new.applymap(lambda x: pattern.sub('', x) if isinstance(x, str) else x)
    
    yesterday = yester.date.today() - yester.timedelta(days=1)
    oldfile='food_daily/petfood_'+yesterday.strftime("%Y%m%d")+'.csv'
    df_old=pd.read_csv(oldfile,dtype='object',keep_default_na=False)
    df_old=df_old.applymap(lambda x: pattern.sub('', x) if isinstance(x, str) else x)

    #異動資料
#     df_add=df_old[df_old['狀態']=='上架'].append(df_new[df_new['狀態']=='上架'])
    df_add=pd.concat([df_old[df_old['狀態']=='上架'],df_new[df_new['狀態']=='上架']],ignore_index=True).reset_index(drop=True)
#     df_drop=df_old[df_old['狀態']=='下架'].append(df_new[df_new['狀態']=='下架'])
    df_drop=pd.concat([df_old[df_old['狀態']=='下架'],df_new[df_new['狀態']=='下架']],ignore_index=True).reset_index(drop=True)

    #新上架資料
    add = df_add.drop_duplicates(subset=['標題','業者名稱'],keep=False).dropna(subset=['標題']).reset_index(drop=True)
    add['新增時間']=datetime.now().strftime("%Y%m%d")
    add['移除時間']=''

    #新下架資料
    drop=df_drop.drop_duplicates(subset=['標題','業者名稱'],keep=False).dropna(subset=['標題']).reset_index(drop=True)
    drop['新增時間']=''
    drop['移除時間']=datetime.now().strftime("%Y%m%d")
#     change=add.append(drop).reset_index(drop=True).iloc[:,1:]
    change=pd.concat([add,drop],ignore_index=True).reset_index(drop=True).iloc[:,1:]

    hostname=""
    dbname=""
    uname=""
    pwd=""
    #Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(host=hostname, db=dbname, user=uname, pw=pwd))
    change.to_sql(pet_food_different, engine, index=False,if_exists='append')
    print('異動資料共{}筆'.format(len(change)))



def Insert2():
    newst='/var/www/html/hong_pet/寵物食品申報網/food_daily/petfood_'+(datetime.now().strftime("%Y%m%d"))+'.csv'
    #連線資料庫
    connection= pymysql.connect(user='',password='',host='',database='',local_infile=1)
    #清空資料庫
    cursor = connection.cursor()
    query1="TRUNCATE TABLE pet_food3"
    cursor.execute(query1)
    connection.commit()
    #插入最新資料
    query2="LOAD DATA LOCAL INFILE '" +newst+ """' INTO TABLE pet_food3  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    IGNORE 1 ROWS;"""
    cursor.execute(query2)
    connection.commit()
    #關閉游標
    cursor.close()
    #關閉資料庫連線
    connection.close()
    print('資料插入pet_food成功')
    
#刪除前2天csv
def Removecsv():
    today2 = yester.date.today() #獲得今天的日期
    yesterday2 = today2 - yester.timedelta(days=2)
    oldfile3='food_daily/petfood_'+yesterday2.strftime("%Y%m%d")+'.csv'
    try:
        os.remove(oldfile3)
        #os.remove(todayfile2)
    except OSError as e:
        print(e)
    else:
        print("刪除{}成功".format(oldfile3))
        
        
if __name__ == '__main__':
    Scrapy()
    try:
        Clear('pet_food_different')
    except:
        print('資料清理錯誤')
        pass
    Insert2()
    Removecsv()