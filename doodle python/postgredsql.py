#!/usr/bin/env python
# -*- encoding: gbk -*-
'''
@File    :   postgredsql.py
@Time    :   2022/01/25 14:22:37
@Author  :   PangMonk
@Contact :   dzqwork@outlook.com
@Desc    :   用python连接postgredsql的代码
'''

# here put the import lib
import psycopg2 
import pandas as pd

import traceback
import psycopg2
import pandas as pd
import os

class Connect():

    # initial constant, 初始化服务器地址、端口、数据库，账户和密码
    def __init__(self, host=None, port=None, db=None, user=None, pw=None):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = pw
        self.query_name = []
        self.query_data = []

    # set up the target database
    def set(self, host=None, port=None, db=None, user=None, pw=None):
        if not host: self.host = host
        if not port: self.port = port
        if not db: self.db = db
        if not user: self.user = user
        if not pw: self.password = pw
    
    def reset(self):
        self.host = None
        self.port = None
        self.db = None
        self.user = None
        self.password = None
        self.query_name = []
        self.query_data = []

    # according to the query fetch data from the target database
    def getQuery(self, name:str, query:str):
        with psycopg2.connect(
            database=self.db,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            sslmode='prefer'
        ) as connect:
            try:
                cur = connect.cursor()
                cur.execute(query)
                header = (",".join([item[0] for item in cur.description])).split(",")
                data = cur.fetchall()

                df = pd.DataFrame(data=data, columns=header)
                cur.close()
                self.query_name.append(name)
                self.query_data.append(df)
            except Exception as e:
                print(e)
                traceback.print_exc()
    
    # according to the querys fetch data from the target database
    def getQuerys(self, names:list, querys:list):
        with psycopg2.connect(
            database=self.db,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            sslmode='prefer'
        ) as connect:
            for name, query in zip(names, querys):
                try:
                    cur = connect.cursor()
                    cur.execute(query)
                    header = (",".join([item[0] for item in cur.description])).split(",")
                    data = cur.fetchall()

                    df = pd.DataFrame(data=data, columns=header)
                    cur.close()
                    self.query_name.append(name)
                    self.query_data.append(df)
                except Exception as e:
                    print(e)
                    traceback.print_exc()

    # after fetching the data, save them to the local
    def save(self, outputfile='./', index=False):
        if os.path.exists(outputfile):
            print("make no direct")
        else: 
            os.mkdir(outputfile)
            print("make dir " + outputfile)

        for name, data in zip(self.query_name, self.query_data):
            print(f'Saving {name}')
            data.to_csv(outputfile + "\\" + name + ".csv", index=index)

if __name__ == '__main__':
    host = 'sample' # 服务器地址
    port = 'sample' # 端口
    db = 'sample' # 数据库
    user = 'sample' # 用户名
    pw = 'sample' # 密码

    name = 'sampleQuery'
    query = """
        select * from table
        limit 100
    """
    conn = Connect(host, port, db, user, pw)
    conn.getQuery(name, query)
    conn.save()

