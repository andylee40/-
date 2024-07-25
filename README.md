---
title: 網路爬蟲 
tags: 工作整理
description: 網路爬蟲
---

# 網路爬蟲與每日自動更新儀表板網頁



### 簡介：
利用 Python 進行網路爬蟲，將資料清洗過後回傳至資料庫，結合 Crontab 排程系統，每日自動執行以上動作，最後將資料視覺化，使用 amCharts 將資料繪製成動態圖表。

<br>

### 工具使用介紹：

:point_right:資料庫：MySQL

:point_right:前端顯示 : HTML、CSS、Boostrap、Javascript

:point_right:後端串連 : PHP

:point_right:網路爬蟲 : Python (BeautifulSoup、Selenium)

<br>

### 網頁展示連結：

http://203.145.215.249/hong_pet/home.php


<br>

## Step1 : 爬取寵物資訊網頁資料

:point_down:利用 Python 爬取以下3個網頁資料

1. 財政部稅籍（https://data.gov.tw/dataset/9400）
![](https://i.imgur.com/YYs6nNK.png)

<br>

2. 寵物食品申報網 （https://petfood.moa.gov.tw/Web/Food）
![](https://imgur.com/kThNjXc.png)
<br>

3. 特定寵物業管理系統 （https://www.pet.gov.tw/Web/BusinessList.aspx）
![](https://imgur.com/VIXMIPy.png)






<br>

## Step2 : 將清洗過後資料匯入資料庫

:point_down:設計儲存資料之資料庫與資料表

![](https://i.imgur.com/A56dzt6.png)

<br>

:point_down:資料表示意圖（Table : pet_manage2）

![](https://i.imgur.com/mlbDNpC.png)



<br>

## Step3 : 結合 Ubuntu 排程系統每日自動執行

:point_down:利用Ubuntu排程系統，每日固定時間執行抓取資料與匯入資料庫之動作

![](https://i.imgur.com/IAl3W0c.png)



<br>

## Step4 : 使用 PHP 串連後端資料庫

:point_down:利用PHP串連MySQL，並將資料整理成指定格式

```php!
<?php
$servername = "localhost";
$username = "帳號";
$password = "密碼";
$dbname = "pet";
$conn = new mysqli($servername, $username, $password, $dbname);
if ($conn->connect_error) 
    {
      die("Connection failed: " . $conn->connect_error);
    }
$conn -> set_charset("utf8");

$sql="SELECT 特定寵物種類,
sum(if(經營業務項目 LIKE '%繁殖%',1,0)) 繁殖,
sum(if(經營業務項目 LIKE '%買賣%',1,0)) 買賣,
sum(if(經營業務項目 LIKE '%寄養%',1,0)) 寄養
from pet_manage2
where 特定寵物種類 !=".'""'." 
GROUP BY 特定寵物種類
order by 繁殖 desc";
$result = mysqli_query($conn, $sql);
$conn->close();
while($row = mysqli_fetch_assoc($result)) 
    {  
    $row['繁殖']=intval($row['繁殖']);
    $row['買賣']=intval($row['買賣']);
    $row['寄養']=intval($row['寄養']);
    $array[] = $row;
    };
$data=json_encode($array, JSON_UNESCAPED_UNICODE);
?>
```

<br>

## Step5 : 使用 HTML & CSS & Javascipt 並結合 amChart 設計網頁

:star:amCharts ： 為開源免費繪製圖表套件，且能使圖表動態化，點選標籤即可顯示或隱藏資料

<br>

![](https://i.imgur.com/U55Y0yX.png)
