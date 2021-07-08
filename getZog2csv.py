# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 07:03:21 2021

@author: zhuangweibin
"""

import requests
from bs4 import BeautifulSoup
import bs4
import csv
import pymysql
from concurrent.futures import ThreadPoolExecutor

# 打开数据库连接
db = pymysql.connect("localhost","root","MYSQL09115295ab99","qileidb" )
#获取游标对象
cursor = db.cursor()
#插入数据语句
query = """insert into zog (creator, name, players,create_time,download_url,zog_id,zog_type) values (%s,%s,%s,%s,%s,%s,%s)"""

f = open('allzog.csv', mode='w', encoding='utf-8')
csvwriter = csv.writer(f)
list = []   # 定义列表用来存放数据        

 
def getHTMLText(url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""
    
     
def main():
    gamename = ""
    creator = ""
    create_time = ""
    brief = ""
    zog_type = ""
    players = ""
    download_url = ""
    for i in range(900,3140):#3140
        url = 'https://www.zillions-of-games.com/cgi-bin/zilligames/submissions.cgi?do=show;id='+str(i)
        download_url = 'https://www.zillions-of-games.com/cgi-bin/zilligames/submissions.cgi?do=download;id='+str(i)
        html = getHTMLText(url)
        soup = BeautifulSoup(html, "html.parser")
    
        #print("prettyhtml:",soup.prettify())
        if(soup.select('td[class="listhead"]')):
            print(i)
            print("不存在")
        else:
            print("存在")
            print(url)
            gamename = soup.select('td[class="gamename"]')[0].get_text()[6:]
            #print(soup.select('td[class="gamename"]')[0].get_text()[6:])#gamename
            for linkup_dflt in soup.select('.linkup_dflt'):
                creator = linkup_dflt.get_text()
                #print("creator:",linkup_dflt.get_text())
                p=linkup_dflt.parent
                #print("parent:",p.get_text())       #作品名 by 作者, 创建时间
                create_time = p.get_text()[-11:]
                if(create_time.count('-')!=2):
                    create_time = "9999-12-31"
                #print("create_time:",p.get_text()[-11:])
            #print(p.find_all('a')[0])#第一个 作品名称 第二个作者 p.find_all('a')[0] p.find_all('a')[1]
            #print(p.find_all('a')[1])
                brief = soup.select('tr[valign="top"]')[-2].get_text()
                #print("描述:",soup.select('tr[valign="top"]')[-2].get_text())#描述
            for pro in soup.select('td[class="properties"]'):
            #print(pro)
            #print(pro.find_all('a'))# 第一个 作品类型 第二个 几人 第三个 下载链接
                zog_type = pro.find_all('a')[0].get_text()
                players = pro.find_all('a')[1].get_text()
                #print("产品类型:",pro.find_all('a')[0].get_text())
                #print("几人玩:",pro.find_all('a')[1].get_text())
                #print("下载链接:",download_url)
        csvwriter.writerow([url,i,gamename,creator,create_time,zog_type,players,download_url])#brief,
        values = (creator, gamename, players,create_time,download_url,i,zog_type)
        list.append(values)
        #print(list)
    cursor.executemany(query, list)
    cursor.close()
    db.commit()
    list.clear()
    db.close()

def download_one_page(i):
    gamename = ""
    creator = ""
    create_time = ""
    brief = ""
    zog_type = ""
    players = ""
    download_url = ""
    url = 'https://www.zillions-of-games.com/cgi-bin/zilligames/submissions.cgi?do=show;id='+str(i)
    download_url = 'https://www.zillions-of-games.com/cgi-bin/zilligames/submissions.cgi?do=download;id='+str(i)
    html = getHTMLText(url)
    soup = BeautifulSoup(html, "html.parser")
    if(soup.select('td[class="listhead"]')):
        print("不存在:",i)
    else:
        #print("存在")
        print(url)
        gamename = soup.select('td[class="gamename"]')[0].get_text()[6:]
            #print(soup.select('td[class="gamename"]')[0].get_text()[6:])#gamename
        for linkup_dflt in soup.select('.linkup_dflt'):
            creator = linkup_dflt.get_text()
            #print("creator:",linkup_dflt.get_text())
            p=linkup_dflt.parent
                #print("parent:",p.get_text())       #作品名 by 作者, 创建时间
            create_time = p.get_text()[-11:]
            if(create_time.count('-')!=2):
                create_time = "9999-12-31"
                #print("create_time:",p.get_text()[-11:])
            #print(p.find_all('a')[0])#第一个 作品名称 第二个作者 p.find_all('a')[0] p.find_all('a')[1]
            #print(p.find_all('a')[1])
            brief = soup.select('tr[valign="top"]')[-2].get_text()
        for pro in soup.select('td[class="properties"]'):
            #print(pro)
            #print(pro.find_all('a'))# 第一个 作品类型 第二个 几人 第三个 下载链接
            zog_type = pro.find_all('a')[0].get_text()
            players = pro.find_all('a')[1].get_text()
    csvwriter.writerow([url,i,gamename,creator,create_time,zog_type,players,download_url])#brief,
    
if __name__ == '__main__':
    with ThreadPoolExecutor(100) as t:
        for i in range(1, 3140): 
            t.submit(download_one_page,i)
        
            
        
