import datetime
import tkinter as tk
from tkinter import ttk

import matplotlib
import pandas as pd
from tksheet import Sheet

from calendarLib import CalendarCustom

matplotlib.use("TKAgg")

pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format


class PageDiary(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        topFrame = tk.Frame(self)

        self.btnMainPage = tk.Button(topFrame, text="主页面", command=self.on_btn_main_page).pack(side=tk.LEFT, padx=4)

        # 日期选择
        # Calendar((x, y), 'ur').selection() 获取日期，x,y为点坐标
        date_start_gain = lambda: [
            self.str_date.set(date)
            for date in [CalendarCustom(None, 'ur').selection()]
            if date]
        tk.Button(topFrame, text='日期:', command=date_start_gain).pack(side=tk.LEFT, padx=4)

        self.str_date = tk.StringVar()
        ttk.Entry(topFrame, textvariable=self.str_date, width=12).pack(side=tk.LEFT)

        today = datetime.date.today()
        self.str_date.set(today)

        tk.Label(topFrame, text="代码：").pack(side=tk.LEFT)
        self.strCode = tk.StringVar(value='')
        ttk.Entry(topFrame, width=10, textvariable=self.strCode).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame, text="名称：").pack(side=tk.LEFT)
        self.strName = tk.StringVar(value='')
        ttk.Entry(topFrame, width=10, textvariable=self.strName).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame, text="操作时间：").pack(side=tk.LEFT)
        self.strTime = tk.StringVar(value='')
        ttk.Entry(topFrame, width=8, textvariable=self.strTime).pack(side=tk.LEFT, padx=4)

        # 操作类型
        self.operateTypeStr = tk.StringVar()
        cmbOperateType = ttk.Combobox(topFrame, width=8, textvariable=self.operateTypeStr, state='readonly')
        # Adding combobox drop down list
        cmbOperateType['values'] = ('买入',
                                    '卖出')
        cmbOperateType.current(0)
        cmbOperateType.pack(side=tk.LEFT, padx=4)
        cmbOperateType.bind("<<ComboboxSelected>>", self.on_cmb_operate_type_select)

        topFrame2 = tk.Frame(self)
        tk.Label(topFrame2, text="价格：").pack(side=tk.LEFT, padx=4, pady=2)
        self.strPrice = tk.StringVar(value='')
        ttk.Entry(topFrame2, width=8, textvariable=self.strPrice).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame2, text="数量：").pack(side=tk.LEFT)
        self.strAmount = tk.StringVar(value='')
        ttk.Entry(topFrame2, width=8, textvariable=self.strAmount).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame2, text="目标价位：").pack(side=tk.LEFT)
        self.strTargetPrice = tk.StringVar(value='')
        ttk.Entry(topFrame2, width=8, textvariable=self.strTargetPrice).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame2, text="止损价位：").pack(side=tk.LEFT)
        self.strStopPrice = tk.StringVar(value='')
        ttk.Entry(topFrame2, width=8, textvariable=self.strStopPrice).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame2, text="收益比：").pack(side=tk.LEFT)
        self.strProfitRate = tk.StringVar(value='')
        ttk.Entry(topFrame2, width=8, textvariable=self.strProfitRate).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame, text="盈利：").pack(side=tk.LEFT)
        self.strProfit = tk.StringVar(value='')
        ttk.Entry(topFrame, width=8, textvariable=self.strProfit).pack(side=tk.LEFT, padx=4)
        tk.Label(topFrame, text="备注：").pack(side=tk.LEFT)
        self.text = tk.Text(topFrame, width=50, height=4)
        self.text.pack(side=tk.LEFT, padx=4)
        tk.Button(topFrame2, text="添加", width=10, command=self.on_btn_add).pack(side=tk.LEFT, padx=4)
        topFrame.pack(side=tk.TOP, fill=tk.BOTH)
        topFrame2.pack(side=tk.TOP, fill=tk.BOTH)

        # 列表框
        self.frameReport = tk.Frame(self)
        self.sheet = Sheet(self.frameReport)
        self.sheet.enable_bindings((
            "single_select",  # "single_select" or "toggle_select"
            "drag_select",  # enables shift click selection as well
            "column_drag_and_drop",
            "row_drag_and_drop",
            "column_select",
            "row_select",
            "column_width_resize",
            "double_click_column_resize",
            "row_width_resize",
            "column_height_resize",
            "arrowkeys",
            "row_height_resize",
            "double_click_row_resize",
            "right_click_popup_menu",
            "rc_select",
            "rc_insert_column",
            "rc_delete_column",
            "rc_insert_row",
            "rc_delete_row",
            "hide_columns",
            "copy",
            "cut",
            "paste",
            "delete",
            "undo",
            "edit_cell"
        ))
        self.sheet.pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.frameReport.pack(side=tk.TOP, fill=tk.BOTH, expand=1, pady=4)
        self.sheet.headers(['日期', '编码', '名称', '操作时间', '操作方向', '价格', '数量', '目标价位', '止损', '收益比', '盈利', '备注'])
        self.sheet.refresh()

    def on_btn_main_page(self):
        self.controller.show_frame('PageMain')
        pass

    def on_btn_add(self):
        self.sheet.insert_row(values=(self.str_date.get(), self.strCode.get(), self.strName.get(), self.strTime.get(),
                                      self.operateTypeStr.get(), self.strPrice.get(), self.strAmount.get(),
                                      self.strTargetPrice.get(),
                                      self.strStopPrice.get(), self.strProfitRate.get(), self.strProfit.get(),
                                      self.text.get('0.0', tk.END)))
        self.sheet.refresh()
        pass

    def on_cmb_operate_type_select(self, event):
        if not self.operateTypeStr.get():
            pass
            # self.nameEntered.delete(0, tk.END)
            # self.nameEntered.insert(0, self.measureTypeStr.get())
        pass

    def tree_solution_selected(self, selected):
        print('tree_solution_selected items:', selected)
        pass
