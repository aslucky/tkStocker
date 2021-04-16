import datetime
import pandas as pd

import tkinter as tk
from tkinter import messagebox, ttk
import TkTreectrl as treectrl
import pymongo
from matplotlib.backends._backend_tk import NavigationToolbar2Tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from numpy import unicode

from app import dbDoc
from calendarLib import CalendarCustom
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("TKAgg")

pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format

class PageCapital(tk.Frame):
    """
    资金曲线绘制
    每日资金记录
    """

    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        # 该 list 内部存放多个子 list，每个子 list 为 [标注点的坐标, 标注]
        self.data_notes = []

        topFrame = tk.Frame(self)

        # 创建一个空的 DataFrame
        self.df_data = pd.DataFrame(columns=['date', 'balance', 'memo'])

        self.btnMainPage = tk.Button(topFrame, text="主页面", command=self.on_btn_main_page).pack(side=tk.LEFT, padx=4)
        # 日期选择
        # Calendar((x, y), 'ur').selection() 获取日期，x,y为点坐标
        date_start_gain = lambda: [
            self.date_start.set(date)
            for date in [CalendarCustom(None, 'ur').selection()]
            if date]
        tk.Button(topFrame, text='日期:', command=date_start_gain).pack(side=tk.LEFT, padx=4)

        self.date_start = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.date_start).pack(side=tk.LEFT)

        today = datetime.date.today()
        self.date_start.set(today)

        tk.Label(topFrame, text="账户余额：").pack(side=tk.LEFT)
        self.strBalance = tk.StringVar(value='')
        ttk.Entry(topFrame, width=15, textvariable=self.strBalance).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame, text="备注：").pack(side=tk.LEFT)
        self.strMemo = tk.StringVar(value='')
        ttk.Entry(topFrame, width=25, textvariable=self.strMemo).pack(side=tk.LEFT, padx=4)

        tk.Button(topFrame, text="添加", width=10, command=self.on_btn_add).pack(side=tk.LEFT, padx=4)

        topFrame.pack(side=tk.TOP, fill=tk.BOTH)

        # 列表框
        frameReport = tk.Frame(self)
        mlbDatasFrame = tk.Frame(frameReport)
        scrollbarBalance = tk.Scrollbar(mlbDatasFrame, orient=tk.HORIZONTAL)
        scrollbarBalanceV = tk.Scrollbar(mlbDatasFrame, orient=tk.VERTICAL)
        self.mlbBalance = treectrl.MultiListbox(mlbDatasFrame, xscrollcommand=scrollbarBalance.set,
                                                yscrollcommand=scrollbarBalanceV.set)
        scrollbarBalance.config(command=self.mlbBalance.xview)
        scrollbarBalance.pack(side=tk.BOTTOM, fill=tk.X)
        scrollbarBalanceV.config(command=self.mlbBalance.yview)
        scrollbarBalanceV.pack(side=tk.RIGHT, fill=tk.Y)
        self.mlbBalance.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.mlbBalance.focus_set()
        self.mlbBalance.configure(selectcmd=self.tree_solution_selected, selectmode='single')
        self.mlbBalance.config(columns=('日期', '余额', '备注'))
        mlbDatasFrame.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        frameReport.pack(side=tk.LEFT, fill=tk.BOTH, expand=0, pady=4)

        # 显示中文标签
        plt.rcParams['font.sans-serif'] = ['SimHei']
        # 显示负号
        plt.rcParams['axes.unicode_minus'] = False
        # 有中文使用 u'中文内容'

        self.draw()
        self.load_data()

    def tree_solution_selected(self, selected):
        print('tree_solution_selected items:', selected)
        pass

    def load_data(self):
        try:
            self.mlbBalance.delete('all')
            coll_capital = dbDoc['capital']
            records = coll_capital.find({}, {"_id": 0, 'date': 1, 'balance': 1, 'memo': 1}).sort('date',
                                                                                                 pymongo.DESCENDING)
            # 列表显示是降序排列
            for data in records:
                self.mlbBalance.insert('end', *map(unicode, (data['date'], data['balance'], data['memo'])))

            # 绘图是升序排列
            records = coll_capital.find({}, {"_id": 0, 'date': 1, 'balance': 1, 'memo': 1}).sort('date',
                                                                                                 pymongo.ASCENDING)
            datas = []
            for data in records:
                datas.append([data['date'], float(data['balance']), data['memo']])
            self.df_data = pd.DataFrame(datas, columns=['date', 'balance', 'memo'])

            self.df_data['date'] = pd.to_datetime(self.df_data['date'], format='%Y-%m-%d')
            self.df_data['date'] = self.df_data['date'].dt.date
            try:
                self.ax.clear()

                # x = self.df_data['date']
                # y = self.df_data['balance']
                # for i in range(len(x)):
                #     # 标注点的坐标
                #     point_x = x[i]
                #     point_y = y[i]
                # #     # point, = plt.plot(point_x, point_y, 'o', c='darkgreen')
                # #     point, = self.ax.plot(point_x, point_y, marker='o', mec='b', mfc='w')
                # #     # 标注框偏移量
                #     offset1 = 40
                #     offset2 = 40
                #     # 标注框
                #     bbox1 = dict(boxstyle="round", fc='lightgreen', alpha=0.6)
                #     # 标注箭头
                #     arrowprops1 = dict(arrowstyle="->", connectionstyle="arc3,rad=0.")
                # #     # 标注
                #     annotation = plt.annotate('{},{}'.format(point_x, point_y), xy=(x[i], y[i]),
                #                               xytext=(-offset1, offset2),
                #                               textcoords='offset points',
                #                               bbox=bbox1, arrowprops=arrowprops1, size=15)
                # #     # 默认鼠标未指向时不显示标注信息
                #     annotation.set_visible(False)
                #     self.data_notes.append([point, annotation])
                #     # point.remove()

                plt.xticks(range(len(self.df_data['date'])), self.df_data['date'], rotation=30)  # rotation表示x轴刻度旋转度数
                self.line, = plt.plot(self.df_data['balance'], marker='o', mec='b', mfc='w')

                locator = matplotlib.dates.AutoDateLocator()
                self.ax.xaxis.set_major_locator(locator)
            except Exception as e:
                import traceback
                traceback.print_exc()
                errMsg = traceback.format_exc()
                #     提示信息
                self.log.error("read contour exception." + errMsg)

            self.ax.autoscale(enable=True, tight=False)
            self.ax.grid(True, linestyle='-.')
        except:
            import traceback
            traceback.print_exc()
            errMsg = traceback.format_exc()
            print(errMsg)
        pass

    # 定义鼠标响应函数
    def on_move(self, event):
        visibility_changed = False
        for point, annotation in self.data_notes:
            should_be_visible = (point.contains(event)[0] == True)

            if should_be_visible != annotation.get_visible():
                visibility_changed = True
                annotation.set_visible(should_be_visible)

        if visibility_changed:
            plt.draw()

    def update_annot(self, ind):
        names = pd.np.array(list("ABCDEFGHIJKLMNO"))
        x, y = self.line.get_data()
        print(ind["ind"][0])
        print(x[ind["ind"][0]])
        self.annot.xy = (40, 40)
        text = "{}".format(" ".join(list(map(str, ind["ind"]))))
        self.annot.set_text(text)
        self.annot.get_bbox_patch().set_alpha(0.4)

    def hover(self, event):

        vis = self.annot.get_visible()
        if event.inaxes == self.ax:
            cont, ind = self.line.contains(event)
            if cont:
                # print(event.)
                self.update_annot(ind)
                self.annot.set_visible(True)
                plt.draw()
            else:
                if vis:
                    self.annot.set_visible(False)
                    plt.draw()

    def draw(self):
        self.fig = plt.figure(num=1)
        self.ax = self.fig.add_subplot(111)

        self.annot = self.ax.annotate("", xy=(0, 0), xytext=(-20, 20), textcoords="offset points",
                                      bbox=dict(boxstyle="round", fc="w"),
                                      arrowprops=dict(arrowstyle="->"))
        self.annot.set_visible(False)

        def format_coord(x, y):
            return 'x=%1.4f, y=%1.4f' % (x, y)

        self.ax.format_coord = format_coord

        self.canvas = FigureCanvasTkAgg(self.fig, self)

        toolbar = NavigationToolbar2Tk(self.canvas, self)
        toolbar.update()
        toolbar.pack(side=tk.TOP, fill=tk.BOTH, expand=False)

        self.canvas.draw()
        self.canvas.get_tk_widget().pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

        # 鼠标移动事件
        on_move_id = self.fig.canvas.mpl_connect('motion_notify_event', self.hover)

        # self.canvas.mpl_connect('key_press_event', toggle_selector)
        # self.canvas.mpl_connect('draw_event', self.onZoomCallback)
        # self.RS.set_active(False)

    def on_btn_add(self):
        if len(self.strBalance.get()) == 0:
            tk.messagebox.showwarning(title='警告', message='缺少余额数据')
            return
        coll_capital = dbDoc['capital']
        record = coll_capital.find_one({'date': self.date_start.get()})
        if record:
            tk.messagebox.showwarning(title='警告', message='指定日期数据已存在')
            return

        result = coll_capital.insert_one(
            {'date': self.date_start.get(), 'balance': self.strBalance.get(), 'memo': self.strMemo.get()})
        # print(result.inserted_id)
        self.load_data()
        pass

    def on_btn_main_page(self):
        self.controller.show_frame('PageMain')
        pass

