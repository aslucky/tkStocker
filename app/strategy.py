import math
import numpy as np
import talib as ta

"""
回调选股：
5弯10
回踩 10日线，25日线
"""


def calc_ma(datas, ma_list):
    """
    根据 ma_list 计算 ma 数值
    calc
    :param datas: dataframe
    :param ma_list: string list
    :return: dataframe 日期从最久开始向前排列
    """
    try:
        datas.sort_values('trade_date', inplace=True)
        for ma in ma_list:
            datas['ma%d' % int(ma)] = ta.EMA(datas['close'].values, int(ma))
    except Exception as e:
        print(e)
    return datas


def back5_10(datas, ma_list):
    """
    5弯10走势， 5日均线 回踩10日均线。
    或者10日 回踩 25日
    5日均线先是和10日拉开距离，然后弯头向下，走平
    :param datas:
    :return:
    """
    datas = calc_ma(datas, ma_list)
    data_list1 = datas['ma%d' % int(ma_list[0])].tolist()
    data_list2 = datas['ma%d' % int(ma_list[1])].tolist()
    name = datas['name'].tolist()[0]
    c = datas['close'].tolist()[-1]
    if data_list1[5] > data_list2[0] * 1.5 and data_list1[0] < data_list2[0] * 1.2:
        [datas['ts_code'].tolist()[0], name, datas['industry'].tolist()[0], c, datas['pct_chg'].tolist()[0]]
    else:
        return None

    pass


def ma25_around(datas, ma_list):
    """
    收盘价在ma25线附近， ma25 > ma43 & & 均线多头排列
    :param datas: 从最新日期开始向后排列
    :param ma_list: 均线天数列表， 比如：[“25”,”43“, "60"] 表示 25附近 并且 25 > 43均线
    :return:
    """
    # check whether has ma column
    datas = calc_ma(datas, [25, 43])
    if len(datas) < 26:
        return None
    name = datas['name'].tolist()[0]
    ma25 = datas['ma25'].tolist()[-1]
    ma43 = datas['ma43'].tolist()[-1]
    # 检查均线多头排列
    if ma25 < ma43:
        return None
    # ma25,ma43 向上
    if len(datas) < 10:
        return None
    ma252 = datas['ma25'].tolist()[-3]
    ma432 = datas['ma43'].tolist()[-25]
    if ma432 is None:
        return None

    angle = get_angle(ma252, ma25)
    angle = math.atan((ma25 / ma252 - 1) * 100) * 180 / 3.1416
    if ma25 < ma252 * 1.01 or ma43 < ma432 * 1.01:
        return None

    c = datas['close'].tolist()[-1]

    # 收盘价在均线上下 2% 范围内，价格低于15元的不考虑
    if ma25 * 0.98 < c < ma25 * 1.02:
        print(f'angle={angle}')
        return [datas['ts_code'].tolist()[0], name, datas['industry'].tolist()[0], c, datas['pct_chg'].tolist()[0]]
    else:
        return None


def get_angle(ma1, ma2):
    """
    使用两点数据计算均线角度，x 默认使用 3天，所以是 3-1
    :param ma1: 前一天的数据点
    :param ma2: 最近一天的数据点
    :return:
    """
    alg1 = np.rad2deg(np.arctan2(ma2 - ma1, 3 - 1))
    alg2 = math.atan((ma2 / ma1 - 1) * 100) * 180 / 3.1416
    print(f"alg1={alg1},alg2={alg2}")
    return alg2
    pass


def ma_nearby(datas, ma_list):
    """
    收盘价在ma25线附近， ma25 > ma43 & & 均线多头排列
    :param datas: 从最新日期开始向后排列
    :param ma_list: 均线天数列表， 比如：[“25”,”43“, "60"] 表示 25附近 并且 25 > 43均线
    :return:
    """
    # check whether has ma column
    datas = calc_ma(datas, ma_list)
    name = datas['name'].tolist()[0]
    # turnover_cur = datas['turnover_rate'].tolist()[-1]
    ma = datas['ma' + ma_list[0]].tolist()[-1]
    # tmp = ma
    # if turnover_cur < 3:
    #     return None
    # 检查均线多头排列
    # for i in range(1, len(ma_list)):
    #     if tmp < datas['ma' + ma_list[i]].tolist()[-1]:
    #         return None
    #     tmp = datas['ma' + ma_list[i]].tolist()[-1]
    # ma25 向上
    if len(datas) < 5:
        return None
    ma1 = datas['ma' + ma_list[0]].tolist()[-1]
    ma2 = datas['ma' + ma_list[0]].tolist()[-5]

    angle = math.atan((ma1 / ma2 - 1) * 100) * 180 / 3.1416
    if ma1 < ma2 * 1.01:
        return None

    c = datas['close'].tolist()[-1]

    # 收盘价在均线上下 5% 范围内，价格低于15元的不考虑
    if ma * 0.98 < c < ma * 1.02:
        print(f'angle={angle}')
        return [datas['ts_code'].tolist()[0], name, datas['industry'].tolist()[0], c, datas['pct_chg'].tolist()[0]]
    else:
        return None


def platform_break_through(datas, params):
    """
    平台突破
    找出90天内创新高的股票，
    :param data: 日线、周线数据
    :param params: 默认 90 天，30周。
    :return: [编码,名称,收盘价,涨幅]
    """
    # 最后一天的收盘价
    name = datas['name'].tolist()[0]
    cur_close = datas['close'].tolist()[0]
    max_close = datas['close'].tail(params).max()
    industry = datas['industry'].tolist()[0]
    # 688 开头的票 跳过，科创板不考虑， 15元以下的不考虑
    if cur_close >= max_close:
        return [datas['ts_code'].tolist()[0], name, industry, cur_close, datas['pct_chg'].tolist()[0]]
    else:
        return None
    pass


def power_stock(datas):
    """
    强势股， 换手率大于上个交易日的2倍，涨幅大于 5%。
    正常应该 3-5天之内会上涨，否则退出，过滤掉偶然一次的上涨
    如果回调低于起涨阳线开盘价，退出

    datas：最少两个交易日的数据
    :return:
    """
    if len(datas) < 2:
        return None
    name = datas['name'].tolist()[0]
    cur_close = datas['close'].tolist()[0]
    turnover_cur = datas['turnover_rate'].tolist()[0]
    turnover_yestoday = datas['turnover_rate'].tolist()[1]
    rise = datas['pct_chg'].tolist()[0]
    if cur_close > 15.00 and rise >= 5 and turnover_cur > turnover_yestoday * 2:
        return [datas['ts_code'].tolist()[0], name, cur_close]
    else:
        return None
    pass


def turnover(datas, ma_list):
    """
    换手率是昨天的 2 倍以上
    5，10，25 均线多头排列
    :param datas:
    :return:
    """
    if len(datas) < 2:
        return None
    datas = calc_ma(datas, ma_list)

    name = datas['name'].tolist()[0]
    cur_close = datas['close'].tolist()[0]
    turnover_cur = datas['turnover_rate'].tolist()[0]
    turnover_yestoday = datas['turnover_rate'].tolist()[1]
    rise = datas['pct_chg'].tolist()[0]
    if turnover_cur > turnover_yestoday * 2:
        return [datas['ts_code'].tolist()[0], name, cur_close]
    else:
        return None
    pass


def daily_report(datas):
    """
    每日涨跌排行统计报告；
    -10>=, -10< x< -7, -7 -4, -4 -1, 0, 4> x >0,7>x>4,10>x>7, >= 10
    :param datas:
    :return:
    """
    pass
