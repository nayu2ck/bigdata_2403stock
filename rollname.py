# !/user/bin/env python3
# -*- coding: utf-8 -*-
import shutil
import pandas as pd
import time
from datetime import datetime
import re
import os
import numpy as np
from tools import *
from 搜索人物或公司 import search_people, df_company, df_hk, sn_hk, map_cols, cols_people, cols_people_hk
import warnings
warnings.filterwarnings("ignore")
pth = "F:/0/0304/"
conpth = "结果3"
rfn = "人物信息管理-标签数据2"
pthfl = f'{pth}{conpth}上市采集.xlsx'
pthrslt = f'{pth}{rfn}结果/'
pthrslt2 = f'{pth}{conpth}/'
pthorg = f'{pth}{rfn}/'

if __name__ == "__main__":
    for k in df_company.keys():
        df_company[k].股票代码 = df_company[k].股票代码.apply(lambda x: f"{x:0>6d}")
    df_hk[sn_hk['company']].证券板块 = "hk"
    df_cp_all = pd.concat(list(df_company.values()) + [df_hk[sn_hk['company']]])

    have = reach_files(pthrslt)
    gs = list(filter(lambda x: '-上市公司匹配.xlsx' in x, have))
    gs.sort(key=lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))

    output = []
    go = input("是否在已有结果基础上采集？0/1")
    if go in ["0", ""]:
        for fn in gs:
            zj = pd.read_excel(pthrslt + fn).dropna(subset=["股票代码"])
            for col in map_cols:
                if col in zj.columns:
                    zj[map_cols[col]] = zj[col]
            zj = splitrowsto1col(zj)
            zj = zj.reset_index()
            zj.drop("index", axis=1, inplace=True)
            out, got_people = search_people(zj, pth + f"{conpth}/{fn.replace('-上市公司匹配', '')}-上市高管匹配.xlsx", w=1)
            out.index = out.KID
            dfg = out.loc[got_people]
            output.append(dfg)

    else:
        have = list(map(lambda x: x.replace('-上市公司匹配.xlsx', ".xlsx").replace('-上市高管匹配.xlsx', ".xlsx"),
                        have))
        have = list(set(have))
        have.sort(key=lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))
        dg = pd.read_excel(pth + f'{rfn}结果多个证券类型.xlsx', sheet_name=None, dtype=str)
        flag = pd.read_excel(pthfl, sheet_name=None)
        output = []
        for fn in have:
            got = pd.read_excel(pthrslt2 + fn + "-上市高管匹配.xlsx", dtype=str)
            output.append(got.loc[got.简历.notna()])
            # rsd = got.loc[got.简历.isna()]
            flg = flag[fn]
            got.index = got.KID
            rsd = got.loc[flg.loc[flg.人物已匹配.apply(qf)].KID].drop_duplicates(subset=["KID"])
            dgone = dg[fn.replace(".xlsx", "-上市公司匹配.xlsx")]
            rsd = pd.merge(rsd, dgone, on="单位", how="inner")
            rsd.drop(columns=list(set(cols_people+cols_people_hk) & set(rsd.columns) - {"股票代码", "姓名"}), inplace=True)
            rsd["股票代码"] = rsd.股票代码_y#.apply(lambda x: f"{x:0>6d}" if type(x) is int else x)
            rsd["证券板块"] = rsd.证券板块_y
            rsd.drop(columns=["股票代码_y", "证券板块_y", '股票代码_x', '证券板块_x'], inplace=True)
            rsd = splitrowsto1col(rsd)
            rsd = rsd.reset_index()
            rsd.drop("index", axis=1, inplace=True)
            rsd.股票代码FD = ""#rsd.股票代码FD.astype(object)
            out, got_people = search_people(rsd, "")
            out.index = out.KID
            dfg = out.loc[got_people]
            if len(dfg):
                dfg = dfg.dropna(subset="简历")
            write_path = pthrslt2 + fn.replace(".xlsx", "-上市高管匹配.xlsx")
            # with pd.ExcelWriter(write_path, mode='a', if_sheet_exists='overlay',
            #                                  engine='openpyxl') as writer:
            #     rows = len(got)
            #     dfg[got.columns].to_excel(writer, startrow=rows, header=rows == 0, index=False)
            pd.concat([got, dfg]).to_excel(write_path, index=False)
            output.append(dfg)
            flg.index = flg.KID
            flg.loc[got_people, "人物已匹配"] = True

    outputdf = pd.concat(output).sort_values(by=["单位", "KID"])
    nan2str = lambda x: "" if pd.isna(x) or x == "nan" else x
    df合并 = pd.merge(outputdf, df_cp_all, left_on="股票代码FD", right_on="股票代码", how="left")
    df合并.index = df合并.KID
    df合并 = df合并.loc[df合并.KID.drop_duplicates().to_list()]
    df合并["_行业"] = df合并.所属证监会行业.apply(nan2str) + df合并.所属行业.apply(nan2str)
    df合并.to_excel(pth + '人物(上市).xlsx', index=False)
    shutil.copyfile(pthfl, pthfl.replace(".xlsx", f"_copy{datetime.now().strftime('%m%d%H%M')}.xlsx"))
    with pd.ExcelWriter(pthfl, mode='a', if_sheet_exists='replace',
                        engine='openpyxl') as writer:
        for k in flag.keys():
            flag[k].to_excel(writer, sheet_name=k, index=False)
    print("done")

