import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.ttk import Combobox
import traceback
import pandas as pd
import collect_stock
class UI():
    def __init__(self):
        self.file_path=''
        self.strvar='运行'
        self.krm=False
    def update(self): #更新可视化界面
        self.window.update()
    def open_file(self): #选择文件
        file_path = filedialog.askopenfilename(filetypes=(("Excel files", "*.xlsx"), ("All files", "*.*")))
        print("选择的文件路径：", file_path)
        self.file_path=file_path

    def write2file(self,hege_data, path):
        # data = pd.read_excel(path)
        # data = pd.concat([data, hege_data], ignore_index=True)
        with pd.ExcelWriter(path, engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
            hege_data.to_excel(writer, index=False)

    def krm_clean(self):
        self.krm=True

    def run(self):
        # 这里可以添加运行按钮的逻辑
        try:
            print("运行按钮被点击")
            input1 = self.entry1.get()
            input2 = self.entry2.get()
            selected_option = self.combo.get()
            input4 = self.entry3.get()
            collect_stock.main(input1,input2,selected_option,input4)

            tk.messagebox.showinfo("提示", "运行结束！")
        # except TypeError as e:
        #     messagebox.showerror("没有该时间范围内的数据", str(e))
        except Exception as e:
            # 捕捉异常，并弹出错误提示窗口
            exc_type, exc_value, exc_traceback=sys.exc_info()
            tb_info = traceback.extract_tb(exc_traceback)
            filename, line, func, text = tb_info[-1]
            # 输出异常信息
            # messagebox.showerror("报错类型：", str(exc_type))
            messagebox.showerror("报错具体信息：", str(exc_value))
            messagebox.showerror("报错文件：", str(filename))
            messagebox.showerror("报错行：", "第"+str(line)+"行")

        finally:
            # 运行结束回归原样
            self.strvar.set("运行")
            return

    def create_UI(self):
        # 创建主窗口
        self.window = tk.Tk()
        # 修改窗口标题
        self.window.title("每月新上市公司采集")
        self.label1 = tk.Label(self.window, text="请输入开始时间:(格式如2023-08-12)")
        self.label1.pack()
        self.entry1 = tk.Entry(self.window)
        self.entry1.pack()

        self.label2 = tk.Label(self.window, text="请输入结束时间:(格式如2023-08-12)")
        self.label2.pack()
        self.entry2 = tk.Entry(self.window)
        self.entry2.pack(pady=10)

        # self.options = ["请选择需要股票类型",'全部', "沪市主板", "科创板",'深市主板','创业板','北交所']
        # self.option_var = tk.StringVar()
        # self.option_var.set(self.options[0])
        # self.option_menu = tk.OptionMenu(self.window, self.option_var, *self.options)
        # self.option_menu.pack()
        self.combo = Combobox(self.window)
        self.combo['values'] = ("请选择需要股票类型",'全部', "沪市主板", "科创板",'深市主板','创业板')
        self.combo.current(0)
        self.combo.pack(pady=10)

        self.label3 = tk.Label(self.window, text="请输入文件保存路径:")
        self.label3.pack()
        self.entry3 = tk.Entry(self.window)
        self.entry3.pack()


        self.strvar = tk.StringVar()
        self.strvar.set('运行')
        submit_button = tk.Button(self.window, textvariable=self.strvar, command=self.run)
        submit_button.pack(pady=10)

        # 设置窗口大小
        self.window.geometry("350x250")
        # 进入主循环
        self.window.mainloop()
app = UI()
app.create_UI()