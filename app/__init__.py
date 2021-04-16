import json

import tushare as ts
import pandas as pd
import pymongo

with open('app/config.json') as json_file:
    data = json.load(json_file)
    ts.set_token(data['token'])

ts_pro = ts.pro_api()

conn = pymongo.MongoClient('127.0.0.1', 27017)
# 用来放股票数据
dbData = conn['stockServiceData']
# 用来放资金曲线，代码表，日历表等
dbDoc = conn['stockServiceDoc']