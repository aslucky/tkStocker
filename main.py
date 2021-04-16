import datetime
import threading
from operator import itemgetter
from tkinter.ttk import Progressbar


import tkinter as tk
from tkinter import messagebox, ttk
import TkTreectrl as treectrl

from numpy import unicode
import pandas as pd
from tksheet import Sheet

from app import dbService
from app.dbService import get_data_by_date
from app.pageCapital import PageCapital
from app.pageDiary import PageDiary
from app.pageScreener import PageScreener
from app.strategy import platform_break_through
from calendarLib import CalendarCustom
import matplotlib

from app.pageTester import PageTester

matplotlib.use("TKAgg")

pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format


class App(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        version = "训练工具 v2020.09.21 0600"
        self.title(version)

        # the container is where we'll stack a bunch of frames
        # on top of each other, then the one we want visible
        # will be raised above the others
        container = tk.Frame(self)
        container.pack(side="top", fill="both", expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        self.center_window()
        # self.wm_iconbitmap('icon.ico')
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        self.frames = {}
        for F in (PageScreener, PageDiary, PageCapital, PageMain, PageTester):
            page_name = F.__name__
            frame = F(parent=container, controller=self)
            self.frames[page_name] = frame

            # put all of the pages in the same location;
            # the one on the top of the stacking order
            # will be the one that is visible.
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame("PageMain")

    def show_frame(self, page_name):
        '''Show a frame for the given page name'''
        frame = self.frames[page_name]
        frame.tkraise()

    def on_closing(self):
        if messagebox.askokcancel("Quit", "是否需要退出程序?"):
            self.quit()
            self.destroy()

    def center_window(self, width=1500, height=768):
        # get screen width and height
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()

        # calculate position x and y coordinates
        x = (screen_width / 2) - (width / 2)
        y = (screen_height / 2) - (height / 2)
        self.geometry('%dx%d+%d+%d' % (width, height, x, y))


class PageMain(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        self.flag = 0
        # 补充数据线程，已运行
        self.FLAG_THRD_FETCH_DATA = 1

        topFrame = tk.Frame(self)
        self.btnDiary = tk.Button(topFrame, text="股市日志", command=self.on_btn_diary).pack(side=tk.LEFT, padx=4)
        tk.Button(topFrame, text="资金日志", command=self.on_btn_capital_diary).pack(side=tk.LEFT, padx=4)
        self.btnFetchData = tk.Button(topFrame, text="补充数据", command=self.on_btn_fetch_data).pack(side=tk.LEFT, padx=4)
        self.chkPowerOffWhenDone = tk.IntVar()
        tk.Checkbutton(topFrame, text="完成后关机", variable=self.chkPowerOffWhenDone).pack(
            side=tk.LEFT, padx=4)

        self.btnScreen = tk.Button(topFrame, text="选股操作", command=self.on_btn_screen).pack(side=tk.LEFT, padx=4)
        self.btnTester = tk.Button(topFrame, text="策略测试", command=self.on_btn_tester).pack(side=tk.LEFT, padx=4)
        self.btnRiseSort = tk.Button(topFrame, text="阶段涨幅排行", command=self.on_btn_rise_sort).pack(side=tk.LEFT, padx=4)

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
        self.date_start.set(today)

        # 结束日期
        date_end_gain = lambda: [
            self.date_end.set(date)
            for date in [CalendarCustom(None, 'ur').selection()]
            if date]
        tk.Button(topFrame, text='结束日期:', command=date_end_gain).pack(side=tk.LEFT, padx=4)

        self.date_end = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.date_end).pack(side=tk.LEFT)

        today = datetime.date.today()
        self.date_end.set(today)

        topFrame.pack(side=tk.TOP, fill=tk.BOTH)

        # 列表框
        self.frameReport = tk.Frame(self)
        mlbDatasFrame = tk.Frame(self.frameReport)
        scrollbarSolution = tk.Scrollbar(mlbDatasFrame, orient=tk.HORIZONTAL)
        scrollbarSolutionV = tk.Scrollbar(mlbDatasFrame, orient=tk.VERTICAL)
        self.mlbSolution = treectrl.MultiListbox(mlbDatasFrame, xscrollcommand=scrollbarSolution.set,
                                                 yscrollcommand=scrollbarSolutionV.set)
        scrollbarSolution.config(command=self.mlbSolution.xview)
        scrollbarSolution.pack(side=tk.BOTTOM, fill=tk.X)
        scrollbarSolutionV.config(command=self.mlbSolution.yview)
        scrollbarSolutionV.pack(side=tk.RIGHT, fill=tk.Y)
        self.mlbSolution.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.mlbSolution.focus_set()
        self.mlbSolution.configure(selectcmd=self.tree_solution_selected, selectmode='single')
        self.mlbSolution.config(columns=('编码', '名称', '涨幅'))
        mlbDatasFrame.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.frameReport.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)
        self.frameReport.pack_forget()

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

    def on_btn_screen(self):
        self.controller.show_frame('PageScreener')
        pass

    def on_btn_diary(self):
        self.controller.show_frame('PageDiary')
        pass

    def on_btn_capital_diary(self):
        self.controller.show_frame('PageCapital')

    def on_btn_tester(self):
        self.controller.show_frame('PageTester')

    def on_btn_fetch_data(self):
        """
        补充数据
        :return:
        """
        self.frameOutput.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)
        self.frameReport.pack_forget()
        if self.FLAG_THRD_FETCH_DATA != self.flag & self.FLAG_THRD_FETCH_DATA:
            thrd = threading.Thread(target=dbService.check_data, args=(self.textOutput,self.chkPowerOffWhenDone.get()))
            thrd.setDaemon(True)  # 守护线程
            thrd.start()
        pass

    def on_btn_rise_sort(self):
        self.frameReport.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)
        self.frameOutput.pack_forget()
        return

        start = self.str_date.get()
        end = self.date_end.get()
        start = start.replace('-', '')
        end = end.replace('-', '')
        df_list = get_data_by_date(start, end)
        rise_list = []
        for it in df_list:
            cmax = it['close'].max()
            cmin = it['close'].min()
            rise = (cmax - cmin) / cmin * 100
            rise_list.append({'code': it['ts_code'].to_list()[0], 'name': it['name'].to_list()[0], 'rise': rise})

        def myFunc(e):
            return e['rise']

        rise_list.sort(key=myFunc, reverse=True)
        print(rise_list)
        for it in rise_list:
            self.mlbSolution.insert('end', *map(unicode, (it['code'], it['name'], it['rise'])))
        pass

    def on_btn_main_page(self):
        self.root.show_frame()
        pass

    def tree_solution_selected(self, selected):
        print('tree_solution_selected items:', selected)
        pass



if __name__ == '__main__':
    root = App()
    root.mainloop()
