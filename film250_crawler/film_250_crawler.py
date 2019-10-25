#!user/bin/python
# -*- coding:UTF-8 -*-
import mysql.connector
import requests
import re
from concurrent.futures import ThreadPoolExecutor
import user_agent
import json
import jsonpath
import random
from lxml import etree
import pymysql
class Film_250_Crawler:
    def __init__(self, mysqlconfig):
        self.mysqlconfig = mysqlconfig
    # 初始化数据库
    def initDataBase(self):
        conn = mysql.connector.connect(**self.mysqlconfig)
        cursor = conn.cursor()
        # 创建数据库 
        creat_sql = "create table films250Content" \
                    "(" \
                        "id                  int(5)       not null primary key," \
                        "link_url             varchar(255) null," \
                        "movieName           varchar(255) null," \
                        "moviePicUrl         varchar(255) null," \
                        "movieRate           float(10,1)    null," \
                        "MovieNum            int(10)       null," \
                        "movieLabel          varchar(255) null," \
                        "Director            varchar(255) null," \
                        "MainAct             varchar(255) null," \
                        "year                varchar(255) null," \
                        "Countries_regions   varchar(255) null," \
                        "Movie_type          varchar(255) null" \
                    ")ENGINE=InnoDB AUTO_INCREMENT=198 DEFAULT CHARSET=utf8;"
        cursor.execute(creat_sql)
        conn.commit()
        # 插入初始化数据，默认值全为null, 除了主键
        init_sql = "INSERT INTO films250Content (id) VALUES (%s);"
        for i in range(250):
            val = (i+1,)           
            print(val)
            cursor.execute(init_sql, val)
        conn.commit()
        cursor.close()
        conn.close()
    def get_url(self,url,each):
        headers= {
            "User-Agent":user_agent.generate_user_agent()
        }
        try:
            response=requests.get(url=url,headers=headers,timeout=5)	
            tree=etree.HTML(response.text)
            tree_list=tree.xpath('//*[@id="content"]/div/div[1]/ol/li')
            movie_list = [] #列表
            #将一个可遍历的数据对象(如列表、元组或字符串)组合为一个索引序列
            for index,ol in enumerate(tree_list):
                movie = dict() #字典
                if each<25:
                    index=index+1
                else:
                    index=index+1+each
                movie['id']=index
                # 链接爬取
                link_url=ol.xpath('./div/div[2]/div[1]/a/@href')
                link_url=link_url[0]
                movie['link_url']=str(link_url)
                # 电影名称爬取
                movieName=ol.xpath('./div[@class="item"]/div[@class="info"]/div[@class="hd"]//a//text()')[1]
                movie['movieName']=str(movieName)
                # 电影URL爬取
                moviePicUrl=ol.xpath('./div[@class="item"]/div[@class="pic"]/a/img/@src')[0]
                movie['moviePicUrl']=str(moviePicUrl)
                # 评分爬取
                movieRate=float(ol.xpath('./div[@class="item"]/div[@class="info"]/div[@class="bd"]/div[@class="star"]/span[2]/text()')[0])
                movie['movieRate']=movieRate
                # 评价人数爬取
                MovieNum=ol.xpath('./div[@class="item"]/div[@class="info"]/div[@class="bd"]/div[@class="star"]/span[4]/text()')[0]
                MovieNum=int(re.sub('\D','',MovieNum))
                movie['MovieNum']=MovieNum
                # 电影标签爬取
                movieLabel=ol.xpath('./div[@class="item"]/div[@class="info"]/div[@class="bd"]/p[@class="quote"]/span[@class="inq"]/text()')
                if len(movieLabel)==1:
                    movieLabel=movieLabel[0]
                else:
                    movieLabel="暂无标签"
                movie['movieLabel']=str(movieLabel)
                # 爬取年份和主演和导演，因为在一个P标签里所以这里先放在一个列表了。
                P_list=ol.xpath('./div[@class="item"]/div[@class="info"]/div[@class="bd"]/p/text()')
                # 去除列表里的空格、空白、以及回车
                P_list=[x.strip() for x in P_list if x.strip() != ''] 
                # 从列表取出导演和主演数据
                Pleft_List=P_list[0]
                if Pleft_List.find('主演',0,len(Pleft_List))>0:
                    Pleft_List=Pleft_List.split('主演',1)
                    # 导演爬取
                    Director=Pleft_List[0]
                    # 主演爬取
                    MainAct='主演'+Pleft_List[1]
                else:
                    # 导演爬取
                    Director=Pleft_List
                    # 主演爬取
                    MainAct='主演:....'
                movie['Director']="".join(Director.split())
                movie['MainAct']="".join(MainAct.split())
                # 从列表取出时间、国家、类型等数据    
                PRight_List=P_list[1]
                PRight_List=PRight_List.split('/',5)
                if len(PRight_List)==3:
                    # 上映年份
                    year=PRight_List[0]
                    # 上映国家或者地区
                    Countries_regions=PRight_List[1]
                    # # 电影类型
                    Movie_type=PRight_List[2]
                else:
                    # 获取国家或者地区年份，由于多于两个以上，所以这里获取第一条
                    year_country=PRight_List[0]
                    # 利用正则替换获取国家或者地区
                    country=re.sub(r'\d','',year_country)
                    # 上映国家或者地区
                    Countries_regions=re.sub(r'\W','',country)
                    # 上映年份
                    year=re.sub(r'\D','',year_country)
                    # 电影类型
                    Movie_type=PRight_List[len(PRight_List)-1]
                movie['Countries_regions']="".join(Countries_regions.split())
                movie['year']="".join(year.split())
                movie['Movie_type']="".join(Movie_type.split())
                movie_list.append(movie)
            # print(movie_list)
            # 更新数据
            conn = mysql.connector.connect(**self.mysqlconfig)
            cursor = conn.cursor()
            update_sql = "UPDATE `films250Content` SET link_url = %s,movieName = %s," \
                            "moviePicUrl = %s,movieRate = %s," \
                            "MovieNum = %s,movieLabel = %s," \
                            "Director=%s,MainAct=%s,"\
                            "year=%s,Countries_regions=%s,Movie_type=%s WHERE id = %s;"
            for item in movie_list:
                val = (item["link_url"], item["movieName"], item["moviePicUrl"],
                        item["movieRate"], item["MovieNum"],
                        item["movieLabel"], item["Director"],
                        item["MainAct"], item["year"],
                        item["Countries_regions"], item["Movie_type"],item["id"])
                # print(val)
                cursor.execute(update_sql, val)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)
    # 更新数据
    def updateInfo(self):
        for each in range(0,250,25):
            try:
                url=r'https://movie.douban.com/top250?start='+str(each)
                self.get_url(url,each)
            except Exception as e:
                print(e) 
if __name__ == '__main__':
    Crawler = Film_250_Crawler("1", "1")
    Crawler.updateInfo()
