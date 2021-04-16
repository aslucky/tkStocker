import os
import time
import tkinter

import numpy as np
import talib as ta
import pymongo
import datetime
import requests
import statsmodels  as sm
from statsmodels import regression

from app import ts_pro, ts, pd

conn = pymongo.MongoClient('127.0.0.1', 27017)
# 用来放股票数据
dbData = conn['stockServiceData']
# 用来放资金曲线，代码表，日历表等
dbDoc = conn['stockServiceDoc']


def log_msg(textOutput, msg):
    textOutput.insert(tkinter.END, msg + '\n')
    textOutput.see(tkinter.END)


def check_friday():
    # 找到最后一个有效的周五交易日
    str_valid_date = get_last_valid_trade_date()
    valid_date = datetime.datetime.strptime(str_valid_date, '%Y%m%d')
    day = valid_date.weekday()
    if day != 4:
        #     不是周五，找到上一个
        if day == 0:
            # 周一
            valid_date = valid_date - datetime.timedelta(days=3)
        elif day == 1:
            # 周二
            valid_date = valid_date - datetime.timedelta(days=4)
        elif day == 2:
            # 周三
            valid_date = valid_date - datetime.timedelta(days=5)
        elif day == 3:
            # 周四
            valid_date = valid_date - datetime.timedelta(days=6)
        elif day == 5:
            # 周六
            valid_date = valid_date - datetime.timedelta(days=1)
        elif day == 6:
            # 周日
            valid_date = valid_date - datetime.timedelta(days=2)

    return valid_date.strftime("%Y%m%d")

def check_1st_month():
    """
    找到每月第一个有效交易日
    """
    # 获取当天的日期，去掉时间
    str_cur_date = datetime.datetime.now().strftime("%Y%m")
    # 转换为日期对象
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m')
    return cur_trade_date.strftime("%Y%m%d")

def check_data(textOutput, isPowerOff):
    """
    检查 日期表，代码表，当日排行榜，
    配置信息： 根据配置状态自动更新 交易日历表，代码表

    calendar: 日历表 yyyy_calender
    code: 代码表 最后一次更新日期，每隔 1 个月更新一次
    trade_data: 最后一次更新日期
    history: 历史数据，最后一次更新日期
    history_week: 历史数据，周线
    curDayList: 当日涨幅排行数据。 最后一次更新日期
    :return: 0 成功；1 失败
    """
    log_msg(textOutput, '开始检查数据')
    collection = dbDoc['config']
    count = collection.find().count()
    if not count:
        # 空配置文件, 插入一条
        result = collection.insert_one(
            {'calendar': '2000_calender', 'trade_data': '200000101', 'history': '20000101', 'history_week': '20000101',
             'history_month': '20000101','code': '20000101', 'curDayList': '20000101'})
        pass
    result = collection.find_one()
    if result['history_month']:
        # 找到月初的第一个交易日
        str_cur_date = check_1st_month()
        # 如果没有该日期的数据，则获取
        # 获取当天的日期，去掉时间
        if str_cur_date != result['history_month']:
            check_history_month(textOutput, str_cur_date)
    if result['calendar']:
        str_date_year = datetime.datetime.now().strftime("%Y")
        table_name = str_date_year + '_calender'
        if result['calendar'] != table_name:
            check_calendar(textOutput)
    if result['code']:
        # 获取当天的日期，去掉时间
        str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
        cur_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
        codeDate = datetime.datetime.strptime(result['code'], '%Y%m%d')
        endDate = codeDate + datetime.timedelta(days=30)
        if endDate < cur_date:
            check_code(textOutput)
    if result['history']:
        # 获取当天的日期，去掉时间
        str_last_trade_date = get_last_valid_trade_date()
        if str_last_trade_date != result['history']:
            check_history(textOutput)
    if result['history_week']:
        # 找到最后一个有效的周五交易日
        str_cur_date = check_friday()
        # 如果没有该日期的数据，则获取
        # 获取当天的日期，去掉时间
        if str_cur_date != result['history_week']:
            check_history_week(textOutput, str_cur_date)
    if result['curDayList']:
        # 获取当天的日期，去掉时间
        str_last_trade_date = get_last_valid_trade_date()
        if str_last_trade_date != result['curDayList']:
            check_cur_day_list(textOutput)
        pass

    log_msg(textOutput, '检查数据完毕')
    if isPowerOff:
        # -t 时间，后面是数字是你要设置的秒数
        os.system('shutdown -s -t 1')
    pass


def check_calendar(textOutput):
    """
    检查交易日期表
    :return:
    """
    log_msg(textOutput, '开始处理交易日历')
    str_date_year = datetime.datetime.now().strftime("%Y")
    table_name = str_date_year + '_calender'
    collection_list = dbDoc.collection_names()
    if table_name not in collection_list:
        while True:
            try:
                calender = ts_pro.trade_cal(start_date=str_date_year + '0101', end_date=str_date_year + '1231')
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                log_msg(textOutput, repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
        collection = dbDoc[table_name]
        # collection.drop()
        dataArr = calender.to_dict('records')
        result = collection.insert_many(dataArr)
        if len(result.inserted_ids) != len(dataArr):
            assert False
            log_msg(textOutput, '插入日期数据失败.请检查...')
        else:
            log_msg(textOutput, '插入日期数据成功...')
            # 更新配置文件记录，没有则插入
            config = dbDoc['config']
            resultConfig = config.find_one()
            config.update({'_id': resultConfig['_id']}, {'$set': {'calendar': table_name}}, True)
    log_msg(textOutput, '完成交易日历数据处理...')


def check_code(textOutput):
    """
    检查代码表
    :return:
    """
    log_msg(textOutput, '开始处理交易代码表...')
    # 查询当前所有正常上市交易的股票列表，沪市和深市分开
    while True:
        try:
            datas = ts_pro.query('stock_basic', exchange='', list_status='L',
                                 fields='ts_code,symbol,name,area,industry,fullname,market,exchange,list_status,list_date,is_hs')
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log_msg(textOutput, repr(e))
            # 不知道为什么偶尔会超时，这里暂停 1 后重试
            time.sleep(1)

    # print(datas)
    data_arr = datas.to_dict('records')
    # 创建集合，相当于表
    t_codes = dbDoc['codes']
    t_codes.drop()
    # 插入获取的代码
    result = t_codes.insert_many(data_arr)
    if len(result.inserted_ids) != len(data_arr):
        assert False
        log_msg(textOutput, '插入代码表数据失败.请检查...')
    else:
        log_msg(textOutput, '插入代码表数据成功...')
        curDate = datetime.datetime.now().strftime("%Y%m%d")

        # 更新配置文件记录，没有则插入
        collectionConfig = dbDoc['config']
        resultConfig = collectionConfig.find_one()
        collectionConfig.update({'_id': resultConfig['_id']}, {'$set': {'code': curDate}}, True)
    log_msg(textOutput, '完成代码表数据处理...')
    pass


def update_history(textOutput):
    """
    接口有时会缺少部分数据，需要更新操作

    :param date:
    :return:
    """
    log_msg(textOutput, '开始处理补充历史数据...')
    # 读取代码表内容，用于遍历获取数据
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    str_cur_date = '20200528'
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    for code in codes:
        # 每个代码创建一个集合
        t_codes = dbData[code['ts_code']]
        start_trade_date = cur_trade_date
        end_date = start_trade_date + datetime.timedelta(days=365)
        if end_date >= cur_trade_date:
            end_date = cur_trade_date
        while start_trade_date <= end_date:
            try:
                log_msg(textOutput,
                        '正在获取 {} {}---{} 数据\n'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                      end_date.strftime('%Y-%m-%d')))
                print('正在获取 {} {}---{} 数据'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                  end_date.strftime('%Y-%m-%d')))
                # 通用行情接口，获取高开低收数据，每次获取365天数据
                datas = ts.pro_bar(ts_code=code['ts_code'], adj='qfq', factors=['tor', 'vr'],
                                   start_date=start_trade_date.strftime('%Y%m%d'),
                                   end_date=end_date.strftime('%Y%m%d'),
                                   retry_count=9999)
                # print(datas.head())
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(repr(e))
                log_msg(textOutput, repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
                continue
            except Exception as e:
                # 未知异常
                log_msg(textOutput, '发现未知异常：' + repr(e))
                print('发现未知异常：' + repr(e))
                time.sleep(1)
                continue
            if datas is not None and not datas.empty:
                datas.drop(['pre_close'], axis=1, inplace=True)
                datas.sort_values('trade_date', inplace=True)
                data_arr = datas.to_dict('records')
                # 插入获取的代码
                result = t_codes.delete_one({'trade_date': data_arr[0]['trade_date']})
                if result.deleted_count != 1:
                    log_msg(textOutput, '删除旧记录失败')
                result = t_codes.insert_many(data_arr)
                if len(result.inserted_ids) != len(data_arr):
                    assert False
                    log_msg(textOutput, '插入日线数据失败.请检查')
            else:
                log_msg(textOutput, '无数据...')
            # 现在接口似乎可以了，如果超时这里再打开
            # time.sleep(0.1)
            # 调整日期
            start_trade_date = end_date + datetime.timedelta(days=1)
            end_date = start_trade_date + datetime.timedelta(days=365)
            if end_date >= cur_trade_date:
                end_date = cur_trade_date
    log_msg(textOutput, '完成补充历史数据处理...')
    pass


def check_history(textOutput):
    """
    从上市日期开始补充历史数据，如果有数据存在，从最后一个交易日开始补充
    :return:
    """
    log_msg(textOutput, '开始处理补充历史数据...')
    # 读取代码表内容，用于遍历获取数据
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    str_cur_date = get_last_valid_trade_date()
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')

    for code in codes:
        # 每个代码创建一个集合
        t_codes = dbData[code['ts_code']]
        # 是否已存在数据
        row_cnt = t_codes.count_documents({}, limit=1)
        # 有数据已存在，获取起始日期，补全数据
        if row_cnt:
            results = t_codes.find().sort('trade_date', pymongo.DESCENDING).limit(1)
            start_trade_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
            print(code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            log_msg(textOutput, code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            start_trade_date = start_trade_date + datetime.timedelta(days=1)
        else:
            # 没有数据存在从上市第一天开始获取数据
            textOutput.insert(tkinter.END, code['ts_code'] + ' 没有数据\n')
            log_msg(textOutput, code['ts_code'] + ' 没有数据')
            print(code['ts_code'] + ' 没有数据')
            start_trade_date = datetime.datetime.strptime(code['list_date'], '%Y%m%d')
        end_date = start_trade_date + datetime.timedelta(days=365)
        if end_date >= cur_trade_date:
            end_date = cur_trade_date
        while start_trade_date <= end_date:
            try:
                log_msg(textOutput,
                        '正在获取 {} {}---{} 数据\n'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                      end_date.strftime('%Y-%m-%d')))
                print('正在获取 {} {}---{} 数据'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                  end_date.strftime('%Y-%m-%d')))
                # 通用行情接口，获取高开低收数据，每次获取365天数据
                datas = ts.pro_bar(ts_code=code['ts_code'], adj='qfq', factors=['tor', 'vr'],
                                   start_date=start_trade_date.strftime('%Y%m%d'),
                                   end_date=end_date.strftime('%Y%m%d'),
                                   retry_count=9999)
                # print(datas.head())
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(repr(e))
                log_msg(textOutput, repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
                continue
            except Exception as e:
                # 未知异常
                print('发现未知异常：' + repr(e))
                log_msg(textOutput, '发现未知异常：' + repr(e))
                time.sleep(1)
                continue
            if datas is not None and not datas.empty:
                datas.drop(['pre_close'], axis=1, inplace=True)
                datas.sort_values('trade_date', inplace=True)
                data_arr = datas.to_dict('records')
                # 插入获取的代码
                result = t_codes.insert_many(data_arr)
                if len(result.inserted_ids) != len(data_arr):
                    assert False
                    log_msg(textOutput, '插入日线数据失败.请检查。')
                    print('插入日线数据失败.请检查。')
            else:
                log_msg(textOutput, '无数据...')
                print('无数据...')
            # 现在接口似乎可以了，如果超时这里再打开
            # time.sleep(0.1)
            # 调整日期
            start_trade_date = end_date + datetime.timedelta(days=1)
            end_date = start_trade_date + datetime.timedelta(days=365)
            if end_date >= cur_trade_date:
                end_date = cur_trade_date

    # 更新配置文件记录，没有则插入
    collectionConfig = dbDoc['config']
    resultConfig = collectionConfig.find_one()
    collectionConfig.update({'_id': resultConfig['_id']}, {'$set': {'history': str_cur_date}}, True)
    print('市场历史数据补全完毕')
    log_msg(textOutput, '市场历史数据补全完毕...')
    pass


def check_history_week(textOutput, str_cur_date):
    """
    从上市日期开始补充历史数据，如果有数据存在，从最后一个交易日开始补充
    :return:
    """
    log_msg(textOutput, '开始处理补充周线历史数据...')
    print('开始处理补充周线历史数据')
    # 读取代码表内容，用于遍历获取数据
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    for code in codes:
        # 每个代码创建一个集合
        t_codes = dbData[code['ts_code'] + '_week']
        # 是否已存在数据
        row_cnt = t_codes.count_documents({}, limit=1)
        # 有数据已存在，获取起始日期，补全数据
        if row_cnt:
            results = t_codes.find().sort('trade_date', pymongo.DESCENDING).limit(1)
            start_trade_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
            log_msg(textOutput, code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            print(code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            start_trade_date = start_trade_date + datetime.timedelta(days=1)
        else:
            # 没有数据存在从上市第一天开始获取数据
            print(code['ts_code'] + ' 没有数据')
            log_msg(textOutput, code['ts_code'] + ' 没有数据')
            start_trade_date = datetime.datetime.strptime(code['list_date'], '%Y%m%d')
        end_date = start_trade_date + datetime.timedelta(days=365)
        if end_date >= cur_trade_date:
            end_date = cur_trade_date
        while start_trade_date <= end_date:
            try:
                log_msg(textOutput, '正在获取 {} {}---{} 周线数据\n'.format(code['ts_code'],
                                                                    start_trade_date.strftime('%Y-%m-%d'),
                                                                    end_date.strftime('%Y-%m-%d')))
                print('正在获取 {} {}---{} 周线数据'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                    end_date.strftime('%Y-%m-%d')))
                # 通用行情接口，获取高开低收数据，每次获取365天数据
                datas = ts.pro_bar(ts_code=code['ts_code'], adj='qfq', factors=['tor', 'vr'],
                                   start_date=start_trade_date.strftime('%Y%m%d'),
                                   end_date=end_date.strftime('%Y%m%d'),
                                   retry_count=9999, freq='W')
                # print(datas.head())
                time.sleep(0.2)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(repr(e))
                log_msg(textOutput, repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
                continue
            except Exception as e:
                # 未知异常
                print('发现未知异常：' + repr(e))
                log_msg(textOutput, '发现未知异常：' + repr(e))
                time.sleep(1)
                continue
            if datas is not None and not datas.empty:
                datas.drop(['pre_close'], axis=1, inplace=True)
                datas.sort_values('trade_date', inplace=True)
                data_arr = datas.to_dict('records')
                # 插入获取的代码
                result = t_codes.insert_many(data_arr)
                if len(result.inserted_ids) != len(data_arr):
                    assert False
                    log_msg(textOutput, '插入周线数据失败.请检查。')
                    print('插入周线数据失败.请检查。')
            else:
                log_msg(textOutput, '无数据...')
                print('无数据...')
            # 现在接口似乎可以了，如果超时这里再打开
            # time.sleep(0.1)
            # 调整日期
            start_trade_date = end_date + datetime.timedelta(days=1)
            end_date = start_trade_date + datetime.timedelta(days=365)
            if end_date >= cur_trade_date:
                end_date = cur_trade_date

    # 更新配置文件记录，没有则插入
    collectionConfig = dbDoc['config']
    resultConfig = collectionConfig.find_one()
    friday = check_friday()
    collectionConfig.update({'_id': resultConfig['_id']}, {'$set': {'history_week': friday}}, True)
    print('市场周线历史数据补全完毕')
    log_msg(textOutput, '市场周线历史数据补全完毕...')
    pass

def check_history_month(textOutput, str_cur_date):
    log_msg(textOutput, '开始处理补充月线历史数据...')
    print('开始处理补充月线历史数据')
    # 读取代码表内容，用于遍历获取数据
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    for code in codes:
        # 每个代码创建一个集合
        t_codes = dbData[code['ts_code'] + '_month']
        # 是否已存在数据
        row_cnt = t_codes.count_documents({}, limit=1)
        # 有数据已存在，获取起始日期，补全数据
        if row_cnt:
            results = t_codes.find().sort('trade_date', pymongo.DESCENDING).limit(1)
            start_trade_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
            log_msg(textOutput, code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            print(code['ts_code'] + ' 已有部分数据截止到: ' + start_trade_date.strftime('%Y%m%d'))
            start_trade_date = start_trade_date + datetime.timedelta(days=1)
        else:
            # 没有数据存在从上市第一天开始获取数据
            print(code['ts_code'] + ' 没有数据')
            log_msg(textOutput, code['ts_code'] + ' 没有数据')
            start_trade_date = datetime.datetime.strptime(code['list_date'], '%Y%m%d')
        end_date = start_trade_date + datetime.timedelta(days=365*10)
        if end_date >= cur_trade_date:
            end_date = cur_trade_date
        while start_trade_date <= end_date:
            try:
                log_msg(textOutput, '正在获取 {} {}---{} 月线数据\n'.format(code['ts_code'],
                                                                    start_trade_date.strftime('%Y-%m-%d'),
                                                                    end_date.strftime('%Y-%m-%d')))
                print('正在获取 {} {}---{} 月线数据'.format(code['ts_code'], start_trade_date.strftime('%Y-%m-%d'),
                                                    end_date.strftime('%Y-%m-%d')))
                # 通用行情接口，获取高开低收数据，每次获取365天数据
                datas = ts.pro_bar(ts_code=code['ts_code'], adj='qfq', factors=['tor', 'vr'],
                                   start_date=start_trade_date.strftime('%Y%m%d'),
                                   end_date=end_date.strftime('%Y%m%d'),
                                   retry_count=9999, freq='M')
                # print(datas.head())
                time.sleep(0.2)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(repr(e))
                log_msg(textOutput, repr(e))
                # 不知道为什么偶尔会超时，这里暂停 1 后重试
                time.sleep(1)
                continue
            except Exception as e:
                # 未知异常
                print('发现未知异常：' + repr(e))
                log_msg(textOutput, '发现未知异常：' + repr(e))
                time.sleep(1)
                continue
            if datas is not None and not datas.empty:
                datas.drop(['pre_close'], axis=1, inplace=True)
                datas.sort_values('trade_date', inplace=True)
                data_arr = datas.to_dict('records')
                # 插入获取的代码
                result = t_codes.insert_many(data_arr)
                if len(result.inserted_ids) != len(data_arr):
                    assert False
                    log_msg(textOutput, '插入月线数据失败.请检查。')
                    print('插入月线数据失败.请检查。')
            else:
                log_msg(textOutput, '无数据...')
                print('无数据...')
            # 现在接口似乎可以了，如果超时这里再打开
            # time.sleep(0.1)
            # 调整日期
            start_trade_date = end_date + datetime.timedelta(days=1)
            end_date = start_trade_date + datetime.timedelta(days=365*10)
            if end_date >= cur_trade_date:
                end_date = cur_trade_date

    # 更新配置文件记录，没有则插入
    collectionConfig = dbDoc['config']
    resultConfig = collectionConfig.find_one()
    collectionConfig.update({'_id': resultConfig['_id']}, {'$set': {'history_month': str_cur_date}}, True)
    print('市场月线历史数据补全完毕')
    log_msg(textOutput, '市场月线历史数据补全完毕...')
    pass

def check_cur_day_list(textOutput):
    """
    从每日数据中提取出来，放到每日涨幅排行列表中，便于查看当日统计信息
    :return:
    """
    log_msg(textOutput, '开始处理涨幅排行榜数据...')
    print('开始处理涨幅排行榜数据')
    str_date_year = datetime.datetime.now().strftime("%Y")
    daily_table_name = '_daily_' + str_date_year
    t_daily = dbData[daily_table_name]
    str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
    # 读取代码表内容，用于遍历获取数据
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    daily_list = []
    for code in codes:
        # 查看 daily 表是否存在当前日期数据
        count = t_daily.count({'trade_date': str_cur_date})
        if count:
            continue
        # 每个代码创建一个集合
        t_stock = dbData[code['ts_code']]
        # 获取指定日期的一条记录
        results = t_stock.find_one({'trade_date': str_cur_date})
        if not results:
            continue
        results.pop('_id')
        daily_list.append(results)
    if not daily_list:
        print('处理完成... 没有数据需要处理')
        log_msg(textOutput, '处理完成... 没有数据需要处理...')
        str_date = datetime.datetime.now().strftime("%Y%m%d")
        config = dbDoc['config']
        resultConfig = config.find_one()
        config.update({'_id': resultConfig['_id']}, {'$set': {'curDayList': str_date}}, True)
        return
    result = t_daily.insert_many(daily_list)
    if len(result.inserted_ids) != len(daily_list):
        assert False
        log_msg(textOutput, '插入日涨幅排行数据失败.请检查...')
        print(r'插入日涨幅排行数据失败.请检查...')
    str_date_year = datetime.datetime.now().strftime("%Y")
    table_name = str_date_year + '_calender'
    # 更新配置文件记录，没有则插入
    config = dbDoc['config']
    resultConfig = config.find_one()
    str_date_year = datetime.datetime.now().strftime("%Y%m%d")
    config.update({'_id': resultConfig['_id']}, {'$set': {'curDayList': str_date_year}}, True)
    print('日涨幅排行数据处理完毕...')
    log_msg(textOutput, '日涨幅排行数据处理完毕...')

def calc_ma(datas, ma_list):
    """
    根据 ma_list 计算 ma 数值
    calc
    :param datas: dataframe
    :param ma_list: string list
    :return: dataframe 日期从前向后排列
    """
    try:
        datas.sort_values('trade_date', inplace=True)
        for ma in ma_list:
            datas['ma%d' % int(ma)] = ta.EMA(datas['close'].values, int(ma))
    except Exception as e:
        print(e)
    return datas

def is_trade_date(str_date):
    """
    判断输入日期是否是交易日
    :param str_date: 字符串类型，格式：YYYYMMDD
    :return:
    """
    str_date_year = datetime.datetime.now().strftime("%Y")
    table_name = str_date_year + '_calender'
    t_calender = dbDoc[table_name]
    result = t_calender.find_one({'cal_date': str_date})
    if result is None:
        return result
    if not result:
        return False
    if result['is_open']:
        return True
    else:
        return False
    pass


def get_last_valid_trade_date():
    """
    获取最后一个有效交易日期
    :return: 返回最后一个有效交易日
    """
    # 获取当天的日期，去掉时间
    str_cur_date = datetime.datetime.now().strftime("%Y%m%d")
    # 转换为日期对象
    cur_trade_date = datetime.datetime.strptime(str_cur_date, '%Y%m%d')
    start_date = cur_trade_date + datetime.timedelta(hours=16)
    cur_date = datetime.datetime.now()
    if cur_date < start_date:
        # 当天 16 点之后有数据，如果当前时间 0 > time < 16 返回 上个交易日日期
        cur_trade_date -= datetime.timedelta(days=1)
    week_day = cur_trade_date.weekday()
    if week_day == 5:
        cur_trade_date = cur_trade_date - datetime.timedelta(days=1)
    elif week_day == 6:
        cur_trade_date = cur_trade_date - datetime.timedelta(days=2)

    # 获取当天的日期，去掉时间
    str_cur_date = cur_trade_date.strftime("%Y%m%d")
    ret = is_trade_date(str_cur_date)
    if ret is None:
        #     没有数据不处理
        pass
    else:
        while not is_trade_date(str_cur_date):
            cur_trade_date = cur_trade_date - datetime.timedelta(days=1)
            str_cur_date = cur_trade_date.strftime("%Y%m%d")
    return cur_trade_date.strftime("%Y%m%d")


def get_codes(lt_date):
    """
    找出时间晚于 lt_date 的
    :param lt_date: yyyymmdd
    :return: [{}]
    """
    t_codes = dbDoc['codes']
    return list(t_codes.find({'list_date': {'$lte': lt_date}}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))


def get_code_count():
    t_codes = dbDoc['codes']
    return t_codes.find().count()


def get_data(count, type, includeST, includeTech):
    """
    获取 count 组周线数据
    :param includeTech: 包括科創板
    :param count:
    :param type: 'w' 周线数据，'d' 日线数据
    :return:
    [df]
    """
    datas = []
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "industry": 1, "list_date": 1}))
    for code in codes:
        if not includeST and -1 != code['name'].find('ST'):
            continue
        if not includeTech and code['ts_code'].startswith('688'):
            continue
        # 每个代码创建一个集合
        if type == 'w':
            t_codes = dbData[code['ts_code'] + '_week']
        elif type == 'd':
            t_codes = dbData[code['ts_code']]
        elif type == 'm':
            t_codes = dbData[code['ts_code'] + '_month']
        #     选择前 count 组数据
        result = t_codes.find().sort('trade_date', pymongo.DESCENDING).limit(count)
        labels = ['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'change', 'pct_chg', 'vol', 'amount',
                  'turnover_rate']
        df = pd.DataFrame(list(result), columns=labels)
        df['name'] = code['name']
        df['industry'] = code['industry']
        if len(df):
            datas.append(df)
    return datas

# 计算走势角度 ， 就是斜率对应的度数
def stocks_data_to_deg(stocklist, start, end):
    deg_data = {}
    for code in stocklist:
        code_data = ts_pro.daily(ts_code=code, start_date=start, end_date=end)
        code_data.fillna(method='bfill', inplace=True)  # 后一个数据填充NAN1
        code_data.index = pd.to_datetime(code_data.trade_date)
        code_data.sort_index(inplace=True)
        code_data.drop(axis=1, columns='trade_date', inplace=True)
        print(code_data.head())
        print(code_data.info())
        try:
            y_arr = code_data.close.values
            x_arr = pd.np.arange(0, len(y_arr))
            x_b_arr = sm.tools.tools.add_constant(x_arr)  # 添加常数列1
            model = regression.linear_model.OLS(y_arr, x_b_arr).fit()  # 使用OLS做拟合
            rad = model.params[1]  # y = kx + b :params[1] = k
            deg_data[code] = np.rad2deg(rad)  # 弧度转换为角度
        except Exception as e:
            import traceback
            traceback.print_exc()
            errMsg = traceback.format_exc()
            print("Error: 异常: " + errMsg)
            pass
    return deg_data

def get_data_thread(count, type, includeST, includeTech, out_q, start_date, sortType=pymongo.DESCENDING):
    """
    获取 count 组周线数据
    :param start_date: YYYYMMDD
    :param includeTech: 包括科創板
    :param count:
    :param type: 'w' 周线数据，'d' 日线数据
    :return:
    [df]
    """
    # data = ts_pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # d = list(data.ts_code)
    all_data = stocks_data_to_deg(['002304.SZ'], '20201012', '20201015')
    print(all_data)

    datas = []
    t_codes = dbDoc['codes']
    codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "industry": 1, "list_date": 1}))
    for code in codes:
        # code: '000001.SZ'
        if not includeST and -1 != code['name'].find('ST'):
            continue
        if not includeTech and code['ts_code'].startswith('688'):
            continue
        if not code['ts_code'].startswith('002304'):
            continue
        # 每个代码创建一个集合
        if type == 'w':
            t_codes = dbData[code['ts_code'] + '_week']
        elif type == 'd':
            t_codes = dbData[code['ts_code']]
        if type == 'm':
            t_codes = dbData[code['ts_code'] + '_month']
        #     选择前 count 组数据
        result = t_codes.find({'trade_date': {'$lte': start_date}}).sort('trade_date', sortType).limit(count)
        labels = ['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'change', 'pct_chg', 'vol', 'amount',
                  'turnover_rate']
        df = pd.DataFrame(list(result), columns=labels)
        df['name'] = code['name']
        df['industry'] = code['industry']
        if len(df):
            out_q.put(df)
    out_q.put(None)
    print('get_data_thread exit ==========')


def get_data_by_date(codes, start, end, type='day', sort=pymongo.DESCENDING):
    """
    支持获取日线，周线数据
    :param codes: 代码表 df
    :param start: 开始日期 YYYYMMDD
    :param end: 结束日期 YYYYMMDD
    :return:
    """
    datas = []
    # t_codes = dbDoc['codes']
    # codes = list(t_codes.find({}, {"_id": 0, "ts_code": 1, "name": 1, "list_date": 1}))
    for code in codes:
        if code['ts_code'].startswith('688'):
            continue
        # 每个代码创建一个集合
        if type == 'day':
            t_codes = dbData[code['ts_code']]
        elif type == 'week':
            t_codes = dbData[code['ts_code'] + '_week']
        else:
            t_codes = dbData[code['ts_code']]
        #     选择前 count 组数据
        result = t_codes.find({'trade_date': {'$gte': start, '$lte': end}}).sort('trade_date', sort)
        if result.count():
            labels = ['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'change', 'pct_chg', 'vol', 'amount',
                      'turnover_rate']
            df = pd.DataFrame(list(result), columns=labels)
            df['name'] = code['name']
            if len(df):
                datas.append(df)

    return datas


def get_data_by_date_thread(codes,start, end, out_q, type='day', sort=pymongo.DESCENDING):
    """

    :param start: 开始日期 YYYYMMDD
    :param end: 结束日期 YYYYMMDD
    :return: [{'ts_code': '000001.SZ', 'name': '平安银行', 'list_date': '19910403'},...]
    """
    print('Enter get_data_by_date_thread ==========')
    for code in codes:
        if code['ts_code'].startswith('688'):
            continue
        # 每个代码创建一个集合
        t_codes = dbData[code['ts_code']]
        if type == 'day':
            t_codes = dbData[code['ts_code']]
        elif type == 'week':
            t_codes = dbData[code['ts_code'] + '_week']
        else:
            t_codes = dbData[code['ts_code']]
        #     选择前 count 组数据
        result = t_codes.find({'trade_date': {'$gte': start, '$lte': end}}).sort('trade_date', sort)
        if result.count():
            labels = ['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'change', 'pct_chg', 'vol', 'amount',
                      'turnover_rate']
            df = pd.DataFrame(list(result), columns=labels)
            df['name'] = code['name']
            if len(df):
                out_q.put(df)
    print('get_data_by_date_thread exit ==========')
