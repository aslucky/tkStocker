import queue
import threading
import tkinter as tk
from operator import itemgetter
from tkinter import ttk
from tkinter.ttk import Progressbar

import matplotlib
import pandas as pd
from tksheet import Sheet
from queue import Queue
import datetime

from app import dbService
from app.dbService import get_code_count, get_last_valid_trade_date
from app.strategy import platform_break_through, ma_nearby, ma25_around
from calendarLib import CalendarCustom

matplotlib.use("TKAgg")

pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format


class PageScreener(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        self.flag = 0
        self.FLAG_THRD_SCREEN_DATA = 1
        # 处理数据队列
        self.queData = Queue()
        # 数据缓存，用于重复使用，
        self.listData = []

        topFrame = tk.Frame(self)

        self.btnMainPage = tk.Button(topFrame, text="主页面", command=self.on_btn_main_page).pack(side=tk.LEFT, padx=4)

        # 日期选择
        # Calendar((x, y), 'ur').selection() 获取日期，x,y为点坐标
        date_start_gain = lambda: [
            self.date_start.set(date)
            for date in [CalendarCustom(None, 'ul').selection()]
            if date]
        self.btnStart = tk.Button(topFrame, text='选股日期:', command=date_start_gain)
        self.btnStart.pack(side=tk.LEFT, padx=4)
        # ret = self.btnStart.winfo_geometry()

        self.date_start = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.date_start).pack(side=tk.LEFT)

        today = get_last_valid_trade_date()
        dt = datetime.datetime.strptime(today, '%Y%m%d')
        today = dt.strftime('%Y-%m-%d')
        self.date_start.set(today)

        tk.Label(topFrame, text="选股类型：").pack(side=tk.LEFT)

        self.screenTypeStr = tk.StringVar()
        cmbScreenType = ttk.Combobox(topFrame, width=15, textvariable=self.screenTypeStr, state='readonly')
        # Adding combobox drop down list
        cmbScreenType['values'] = ('平台突破', '均线附近')
        cmbScreenType.current(1)
        cmbScreenType.pack(side=tk.LEFT, padx=4)
        # cmbScreenType.bind("<<ComboboxSelected>>", self.on_cmb_screen_select)
        # 选股周期数
        self.screenIntervalCountStr = tk.StringVar(value='25')
        # state=tk.DISABLED 默认禁止输入
        self.screenIntervalCount = ttk.Entry(topFrame, width=10, textvariable=self.screenIntervalCountStr)
        self.screenIntervalCount.pack(side=tk.LEFT, padx=4)

        # 选股周期类型
        self.screenIntervalTypeStr = tk.StringVar()
        cmbScreenInterval = ttk.Combobox(topFrame, width=8, textvariable=self.screenIntervalTypeStr, state='readonly')
        # Adding combobox drop down list
        cmbScreenInterval['values'] = ('日',
                                       '周',
                                       '月')
        cmbScreenInterval.current(0)
        cmbScreenInterval.pack(side=tk.LEFT, padx=4)
        # cmbScreenInterval.bind("<<ComboboxSelected>>", self.on_cmb_screen_interval_select)

        self.chkST = tk.IntVar()
        tk.Checkbutton(topFrame, text="包括ST", variable=self.chkST).pack(
            side=tk.LEFT, padx=4)

        self.chkTech = tk.IntVar()
        tk.Checkbutton(topFrame, text="包括科创板", variable=self.chkTech).pack(
            side=tk.LEFT, padx=4)

        self.btnStart = tk.Button(topFrame, text="选股", command=self.on_btn_start)
        self.btnStart.pack(side=tk.LEFT, padx=4)
        topFrame.pack(side=tk.TOP, fill=tk.BOTH)

        self.tipsStr = tk.StringVar()
        tk.Label(topFrame, textvariable=self.tipsStr, font=("simsun", 12)).pack(side=tk.LEFT)
        self.tipsStr.set('状态：准备...')

        # Progress bar widget
        self.progress = Progressbar(topFrame, orient=tk.HORIZONTAL, length=100, mode='determinate')
        # self.progress.pack(side=tk.LEFT, padx=4)
        # self.progress.pack_forget()

        # 列表框
        self.frameReport = tk.Frame(self)
        self.sheet = Sheet(self.frameReport)
        self.sheet.enable_bindings((
            # "single_select",  # "single_select" or "toggle_select"
            # "drag_select",  # enables shift click selection as well
            # "column_drag_and_drop",
            # "row_drag_and_drop",
            # "column_select",
            "row_select",
            "column_width_resize",
            "double_click_column_resize",
            # "row_width_resize",
            # "column_height_resize",
            # "arrowkeys",
            # "row_height_resize",
            # "double_click_row_resize",
            # "right_click_popup_menu",
            # "rc_select",
            # "rc_insert_column",
            # "rc_delete_column",
            # "rc_insert_row",
            # "rc_delete_row",
            # "hide_columns",
            # "copy",
            # "cut",
            # "paste",
            # "delete",
            # "undo",
            # "edit_cell"
        ))
        self.sheet.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.frameReport.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)
        self.sheet.headers(['编码', '名称', '板块', '收盘价', '涨幅'])
        self.sheet.refresh()

    def on_btn_main_page(self):
        self.controller.show_frame('PageMain')
        pass



    def on_btn_start(self):
        if self.FLAG_THRD_SCREEN_DATA == self.flag & self.FLAG_THRD_SCREEN_DATA:
            return
        self.flag |= self.FLAG_THRD_SCREEN_DATA
        self.progress.pack(side=tk.LEFT, padx=4)
        # 获取数据线程，取一个放入队列，由另一个线程处理
        # 周期参数
        iInterval = int(self.screenIntervalCountStr.get())
        # TODO 测试代码，
        iInterval = 43
        type = 'd'
        if self.screenIntervalTypeStr.get() == '周':
            type = 'w'
        elif self.screenIntervalTypeStr.get() == '月':
            type = 'm'

        start_date = self.date_start.get().replace('-','')
        # 多获取 10 组数据
        thrd = threading.Thread(target=dbService.get_data_thread,
                                args=(iInterval + 30, type, self.chkST.get(), self.chkTech.get(), self.queData,start_date,))
        thrd.setDaemon(True)  # 守护线程
        thrd.start()

        cnt = self.sheet.get_total_rows()
        for i in range(cnt):
            self.sheet.delete_row(i)
        self.sheet.refresh()

        if self.screenTypeStr.get() == '平台突破':
            thrd = threading.Thread(target=self.screen_platform, args=(self.queData,))
            thrd.setDaemon(True)  # 守护线程
            thrd.start()
        elif self.screenTypeStr.get() == '均线附近':
            thrd = threading.Thread(target=self.screen_ma_around, args=(self.queData,))
            thrd.setDaemon(True)  # 守护线程
            thrd.start()
        pass

    def screen_platform(self, in_q):
        """
        平台突破
        找出N天内创新高的股票，
        :return:
        """
        self.btnStart['state'] = 'disabled'
        self.tipsStr.set('状态：正在读取数据，请耐心等待...')

        # 准备数据
        iInterval = int(self.screenIntervalCountStr.get())
        type = 'd'
        if self.screenIntervalTypeStr.get() == '周':
            type = 'w'
        elif self.screenIntervalTypeStr.get() == '月':
            type = 'm'
        self.progress['value'] = 5
        # root.update_idletasks()
        # 避免中间缺数据 * 2
        count = iInterval + 5
        datas = dbService.get_data(count, type, self.chkST.get(), self.chkTech.get())
        screenCount = 0
        self.tipsStr.set('状态：正在选股，请耐心等待...')

        per_interval = len(datas) / 95
        step_count = 0
        progress_step = 5
        pickup_list = []
        for it in datas:
            step_count += 1
            if step_count >= per_interval:
                step_count = 0
                progress_step += 1
                self.progress['value'] = progress_step
                # root.update_idletasks()
            result = platform_break_through(it, iInterval)
            if result:
                screenCount += 1
                print(result)
                pickup_list.append(result)

        pickup_sorted = sorted(pickup_list, key=itemgetter(3), reverse=True)

        for it in pickup_sorted:
            self.sheet.insert_row(values=it)

        self.sheet.refresh()
        self.progress['value'] = 100
        # root.update_idletasks()
        self.tipsStr.set('状态：共选出 {:d} 只股票'.format(screenCount))
        self.flag &= (~self.FLAG_THRD_SCREEN_DATA)
        self.btnStart['state'] = 'normal'
        self.progress.pack_forget()
        print("screen_platform exit ==========")
        pass

    def screen_ma_around(self, in_q):
        """
        参数 120,2 表示 120 日均线，2%附近  ma120*0.98 < 收盘价 < ma120*1.2
        :return:
        """
        self.btnStart['state'] = 'disabled'

        screenCount = 0
        self.tipsStr.set('状态：正在处理，请耐心等待...')

        # 准备数据
        param = [(self.screenIntervalCountStr.get())]
        per_interval = get_code_count() / 95
        step_count = 0
        progress_step = 5
        pickup_list = []
        while True:
            try:
                it = in_q.get_nowait()
            except queue.Empty as e1:
                continue
            if it is None:
                break
            step_count += 1
            if step_count >= per_interval:
                step_count = 0
                progress_step += 1
                self.progress['value'] = progress_step
            result = ma25_around(it, param)
            if result:
                screenCount += 1
                print(result)
                self.sheet.insert_row(values=result)
                # self.sheet.refresh()
                # pickup_list.append(result)

        # pickup_sorted = sorted(pickup_list, key=itemgetter(3), reverse=True)
        #
        # for it in pickup_sorted:
        #     self.sheet.insert_row(values=it)

        self.progress['value'] = 100
        # root.update_idletasks()
        self.tipsStr.set('状态：共选出 {:d} 只股票'.format(screenCount))
        self.flag &= (~self.FLAG_THRD_SCREEN_DATA)
        self.btnStart['state'] = 'normal'
        self.progress.pack_forget()
        self.progress['value'] = 0
        print("screen_ma_around exit ==========")
        pass

    def tree_solution_selected(self, selected):
        print('tree_solution_selected items:', selected)
        pass


