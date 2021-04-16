import datetime
import math
import queue
import threading
import time
import tkinter as tk
from queue import Queue
from tkinter import messagebox, ttk
import pandas as pd
from app import dbService
from app.dbService import get_data_by_date, get_codes, get_data_by_date_thread, calc_ma
from app.strategy import ma25_around
from calendarLib import CalendarCustom


class BuyStrategy25:
    """
    回踩25周均线策略：

    25，43线向上
    25 > 43
    价格回落到 25 线附近 3% 左右，收盘价大于25线
    收盘价呈上涨趋势，买入点前25个交易日涨幅低于30%

    10 < 乖离率25 盈利卖出
    -4 > 乖离率25 亏损卖出
    """

    def __init__(self):
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self, s, textOutput, in_q):
        buy_list = []
        win_count = 0
        lose_count = 0
        win_max = 0.0
        lose_max = 0.0
        while not self._stop_event.is_set() or not in_q.empty():
            try:
                data = in_q.get(block=False)
            except queue.Empty:
                time.sleep(1)
                continue
            except Exception as e:
                import traceback
                traceback.print_exc()
                errMsg = traceback.format_exc()
                print("Error: " + errMsg)
                time.sleep(1)
                continue
            # print('check ' + ' ' + data['ts_code'].to_list()[0])
            # 准备ma25周均线数据
            df = calc_ma(data, ['25','43'])
            df = df[df['ma25'].notna()]
            df = df[df['ma43'].notna()]
            counts = len(df) - 1
            if counts < 5:
                continue
            for idx in range(counts-5, 0, -1):
                skip = False
                ma25 = df.at[idx, 'ma25']
                ma43 = df.at[idx, 'ma43']
                c = df.at[idx, 'close']
                if ma25<ma43 or ma25 < df.at[idx+5, 'ma25'] * 1.02 or df.at[idx, 'ma43'] < df.at[idx+5, 'ma43'] * 1.01:
                    skip = True
                elif ma25 * 0.97 < c < ma25 * 1.03:
                    # 度数:(ATAN((DFA/REF(DFA,1)-1)*100)*180/3.14115926);
                    angle_norm = math.atan(
                        (df.at[idx, 'ma25'] / df.at[idx + 5, 'ma25'] - 1) * 100) * 180 / 3.1415926

                else:
                    skip=True
                if skip:
                    continue

                # 看看持有一年结果如何
                diff = df.at[0, 'close'] - df.at[idx, 'close']
                if (df.at[idx, 'close'] < df.at[0, 'close']):
                    dc = {'date': df.at[idx, 'trade_date'], 'code': df.at[idx, 'ts_code'],
                          'close': df.at[idx, 'close'],
                          'last_close': df.at[0, 'close'], 'ma25': df.at[idx, 'ma25'], 'win': 1,
                          'name': df.at[idx, 'name'], 'angle_norm': angle_norm, 'priceDiff': diff}
                    win_count += 1
                    if (diff > win_max):
                        win_max = diff
                else:
                    dc = {'date': df.at[idx, 'trade_date'], 'code': df.at[idx, 'ts_code'],
                          'close': df.at[idx, 'close'],
                          'last_close': df.at[0, 'close'], 'ma25': df.at[idx, 'ma25'], 'win': 0,
                          'name': df.at[idx, 'name'], 'angle_norm': angle_norm, 'priceDiff': diff}
                    lose_count += 1
                    if (diff < lose_max):
                        lose_max = diff
                buy_list.append(dc)
                textOutput.insert(tk.END, f'{dc}\n')
                print(dc)
                break

        print(
            f'buy_strategy exit ========== win={win_count},lose={lose_count},per={win_count / (win_count + lose_count) * 100},'
            f'win_max={win_max},lose_max={lose_max}')
        labels = ['date',  'code', 'close', 'last_close', 'ma25', 'win', 'name', 'angle_norm',
                  'priceDiff']
        df = pd.DataFrame(buy_list, columns=labels)
        df.to_csv('stock.csv', encoding='utf_8_sig')



class PageTester(tk.Frame):
    """
    用于测试和验证各种策略
    """

    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        self.flag = 0
        # 补充数据线程，已运行
        self.FLAG_THRD_FETCH_DATA = 1

        topFrame = tk.Frame(self)
        self.btnMainPage = tk.Button(topFrame, text="主页面", command=self.on_btn_main_page).pack(side=tk.LEFT, padx=4)
        self.testTypeStr = tk.StringVar()
        measureType = ttk.Combobox(topFrame, width=15, textvariable=self.testTypeStr, state='readonly')
        # Adding combobox drop down list
        measureType['values'] = ('25策略', '涨停概率',
                                 '待添加项')
        measureType.current(0)
        measureType.pack(side=tk.LEFT, padx=4)
        self.btnStart = tk.Button(topFrame, text="开始测试", command=self.on_btn_start).pack(side=tk.LEFT, padx=4)

        # 日期选择
        # Calendar((x, y), 'ur').selection() 获取日期，x,y为点坐标
        date_start_gain = lambda: [
            self.date_start.set(date)
            for date in [CalendarCustom(None, 'ur').selection()]
            if date]
        tk.Button(topFrame, text='开始日期:', command=date_start_gain).pack(side=tk.LEFT, padx=4)

        self.date_start = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.date_start).pack(side=tk.LEFT)

        today = datetime.date.today()
        # TODO test only here
        today = datetime.date(2020, 1, 1)
        self.date_start.set(today)

        # 结束日期
        date_end_gain = lambda: [
            self.date_end.set(date)
            for date in [CalendarCustom(None, 'ur').selection()]
            if date]
        tk.Button(topFrame, text='结束日期:', command=date_end_gain).pack(side=tk.LEFT, padx=4)

        self.date_end = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.date_end).pack(side=tk.LEFT)

        # TODO test only here
        today = datetime.date(2021, 1, 1)
        self.date_end.set(today)

        topFrame.pack(side=tk.TOP, fill=tk.BOTH)

        # 补充数据输出记录
        self.frameOutput = tk.Frame(self)
        self.textOutput = tk.Text(self.frameOutput)
        self.textOutput.pack(side=tk.LEFT, fill=tk.BOTH, expand=1)
        scrollbarOutputV = tk.Scrollbar(self.frameOutput, orient=tk.VERTICAL)
        scrollbarOutputV.config(command=self.textOutput.yview)
        scrollbarOutputV.pack(side=tk.RIGHT, fill=tk.Y)
        self.textOutput.config(yscrollcommand=scrollbarOutputV.set)
        self.frameOutput.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)

        frameStatus = tk.Frame(self)
        self.statusStr = tk.StringVar()
        self.status = tk.Label(frameStatus, textvariable=self.statusStr, height=1, anchor='w', fg="black",
                               font=("simsun", 12))
        self.status.pack(side=tk.BOTTOM, fill=tk.X, expand=1)
        frameStatus.pack(side=tk.BOTTOM, fill=tk.X, expand=0)
        self.statusStr.set('状态：准备...')

    def rise_limit_rate(self, start, end, textOutput):
        """
        测试选定时间段内，当天涨停之后，第二天的走势
        涨跌幅 0-5,5-10,10<

        没有去掉新股上市第一天的情况
        2018.02.09-2018.12.28 大盘 下跌期间的统计
        上涨=1336,下跌=661
        各自总量占比
        涨幅0-5=35.85,涨幅5-10=18.68,涨幅10以上=12.37,
        跌幅0-5=28.14,跌幅5-10=4.56,跌幅10以上=0.40
        :return:
        """
        textOutput.insert(tk.END, '正在载入数据\n')
        dtStart = datetime.datetime.strptime(start, '%Y%m%d')
        dtStart -= datetime.timedelta(days=5)
        code_start = dtStart.strftime('%Y%m%d')
        # 只处理 上市5天之后的股票
        df_codes = get_codes(code_start)
        df_list = get_data_by_date(start, end)
        rise05 = 0
        rise510 = 0
        rise10 = 0
        down05 = 0
        down510 = 0
        down10 = 0
        textOutput.insert(tk.END, '正在处理数据\n')
        strInfo = ""
        for df in df_list:
            # 去掉上市第一天的 涨停情况
            ts_code = df.at[0, 'ts_code']
            if not ts_code in df['ts_code'].values:
                print('{} list date less than start date'.format(ts_code))
                continue
                pass
            cnt = len(df) - 1
            isFound = False
            for i in range(cnt, 0, -1):
                row = df.loc[[i]]
                if not isFound and row.at[i, 'pct_chg'] >= 10.0:
                    # 找到涨停板，看第二个交易日的涨幅
                    isFound = True
                    strInfo += row.to_string() + '\n'
                    continue
                if isFound:
                    if row.at[i, 'pct_chg'] >= 0.0 and row.at[i, 'pct_chg'] <= 5.0:
                        rise05 += 1
                    elif row.at[i, 'pct_chg'] > 5.0 and row.at[i, 'pct_chg'] <= 10.0:
                        rise510 += 1
                    elif row.at[i, 'pct_chg'] > 10.0:
                        rise10 += 1
                    elif row.at[i, 'pct_chg'] < 0.0 and row.at[i, 'pct_chg'] >= -5.0:
                        down05 += 1
                    elif row.at[i, 'pct_chg'] < -5.0 and row.at[i, 'pct_chg'] >= -10.0:
                        down510 += 1
                    elif row.at[i, 'pct_chg'] < -10.0:
                        down10 += 1
                    isFound = False
                    break

        riseCount = rise05 + rise510 + rise10
        downCount = down05 + down510 + down10
        count = riseCount + downCount

        textOutput.insert(tk.END,
                          '上涨计数={},占比{},下跌计数={},占比{}\n'.format(riseCount, riseCount / (riseCount + downCount) * 100,
                                                               downCount, downCount / (riseCount + downCount) * 100))
        textOutput.insert(tk.END,
                          '涨幅0-5={:.2f},涨幅5-10={:.2f},涨幅10以上={:.2f},跌幅0-5={:.2f},跌幅5-10={:.2f},跌幅10以上={:.2f}\n'.format(
                              rise05,
                              rise510,
                              rise10,
                              down05,
                              down510,
                              down10))
        if riseCount:
            textOutput.insert(tk.END, '上涨占比 rise05={:.2f},rise510={:.2f},rise10={:.2f}\n'.format(
                rise05 / riseCount * 100, rise510 / riseCount * 100, rise10 / riseCount * 100))
        if downCount:
            textOutput.insert(tk.END, '下跌占比 down05={:.2f},down510={:.2f},down10={:.2f}\n'.format(
                down05 / downCount * 100, down510 / downCount * 100, down10 / downCount * 100))
        textOutput.insert(tk.END,
                          '各自总量占比 涨幅0-5={:.2f},涨幅5-10={:.2f},涨幅10以上={:.2f}\n'.format(
                              rise05 / count * 100, rise510 / count * 100, rise10 / count * 100))
        textOutput.insert(tk.END,
                          '各自总量占比 跌幅0-5={:.2f},跌幅5-10={:.2f},跌幅10以上={:.2f},\n'.format(
                              down05 / count * 100, down510 / count * 100, down10 / count * 100))
        textOutput.insert(tk.END, '处理结束...')

        textOutput.insert(tk.END, strInfo)
        self.flag &= (~self.FLAG_THRD_FETCH_DATA)
        pass

    def on_test_m25(self, start, end, textOutput):
        textOutput.insert(tk.END, '正在载入数据\n')
        dtStart = datetime.datetime.strptime(start, '%Y%m%d')
        dtStart -= datetime.timedelta(weeks=30)
        code_start = dtStart.strftime('%Y%m%d')
        # 只处理 上市5天之后的股票
        df_codes = get_codes(code_start)
        q = Queue()
        bs = BuyStrategy25()
        t1 = threading.Thread(target=bs.run, args=([], textOutput, q))
        t2 = threading.Thread(target=dbService.get_data_by_date_thread, args=(df_codes, code_start, end, q, 'week'))
        t1_start = time.process_time()
        t2.start()
        t1.start()
        t2.join()
        bs.stop()
        t1.join()
        # Stop the stopwatch / counter
        t1_stop = time.process_time()
        print("Elapsed time during the whole program in seconds:",
              t1_stop - t1_start)
        self.flag &= (~self.FLAG_THRD_FETCH_DATA)
        pass

    def on_btn_start(self):
        if self.FLAG_THRD_FETCH_DATA == self.flag & self.FLAG_THRD_FETCH_DATA:
            return
        self.flag |= self.FLAG_THRD_FETCH_DATA
        start = self.date_start.get()
        end = self.date_end.get()
        start = start.replace('-', '')
        end = end.replace('-', '')
        self.textOutput.delete('1.0', tk.END)
        if self.testTypeStr.get() == "涨停概率":
            self.textOutput.insert(tk.END, '开始涨停后第二天走势概率统计：\n')
            self.textOutput.insert(tk.END, '{}-{}：\n'.format(start, end))
            thrd = threading.Thread(target=self.rise_limit_rate, args=(start, end, self.textOutput,))
            thrd.setDaemon(True)  # 守护线程
            thrd.start()
        elif self.testTypeStr.get() == '25策略':
            self.textOutput.insert(tk.END, '25周线附近操作概率统计：\n')
            self.textOutput.insert(tk.END, '{}-{}：\n'.format(start, end))
            thrd = threading.Thread(target=self.on_test_m25, args=(start, end, self.textOutput,))
            thrd.setDaemon(True)  # 守护线程
            thrd.start()

    def on_btn_main_page(self):
        self.controller.show_frame('PageMain')
        pass
