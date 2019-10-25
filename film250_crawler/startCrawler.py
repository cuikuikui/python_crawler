#!user/bin/python
# -*- coding:UTF-8 -*-
import time
import mysql.connector
from film_250_crawler import Film_250_Crawler

def main():
    print("开始部署爬虫组")
    print("Set mysql config:")
    config = {
        'user': 'root',
        'password': '123456',
        'host': '47.102.124.213',
        'port': '3306',
        'database':'DataSet',
        'charset':'utf8'
    }
    print("Set static folder:")
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.close()
        conn.close()
        print("mysql test successful!")
    except Exception as err:
        print(str(err))
        return
    isSet = "N"
    # 数据库初始化
    isSet = input('Do you want to initialize the database? (Y/N)')
    while isSet != 'Y' and isSet != 'N' and isSet != 'y' and isSet != 'n':
       isSet = input('Do you want to initialize the database? (Y/N)')

    film_250_crawler = Film_250_Crawler(config)
    if isSet == 'Y' or isSet == 'y':
        film_250_crawler.initDataBase()
    while True:
        # 爬取豆瓣前250条数据信息
        film_250_crawler.updateInfo()
        # 定时一天更新一次数据
        time.sleep(60*60*24)
if __name__ == '__main__':
    main()
