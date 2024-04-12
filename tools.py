# !/user/bin/env python3
# -*- coding: utf-8 -*-
import re
import pandas as pd
import os


def spillt(t: str, s):
    try:
        if type(t)==list:
            r = []
            for tt in t:
                if '.' in tt[1:][:-1]:
                    if str.isdigit(tt[tt.index('.')-1]) and str.isdigit(tt[tt.index('.')+1]):
                        r.append(s)
                        continue
                r += tt.split(s)
            return r
        for sp in s:
            t = spillt([t] if type(t)==str else t, sp)
        while type(t) == list and '' in t:  # refs == ['']
            t.remove('')
        return t if type(t)==list else [t]
    except:
        print('False Value ', t)
        return t


def yx(s: set, yl):
    l = list(s)
    l.sort(key=lambda x: yl.index(x))
    return l


def is_school(t):
    if pd.isna(t):
        return False
    if is_otype(t):
        return True
    return re.search('大学|学院|[医科信南北工][大]', t) is not None


def is_otype(t):
    if pd.isna(t):
        return False
    return re.search('医院|委员|学会|会议|歌手|导演|演员|作家|画家|中心|政府|市政|党组|常委', t) is not None

def qf(x):
    return not x


def reach_files(pth, topdown=True):
    if not os.path.isdir(pth):
        print('不存在目录')
        return []
    for root, dirs, files in os.walk(pth, topdown):
        if len(files) > 0:
            break
    return files


def splitrows(df, splits=10):
    pis = df.股票代码.apply(lambda x: len(x.split("|")))
    maxam = pis[pis < splits].max()
    for i in range(1, maxam + 1):
        df[f"股票代码_{i}"] = df.股票代码.apply(
            lambda x: yx(set(x.split("|")), x.split("|"))[i - 1] if len(x.split("|")) >= i else "")
    l = [df.loc[df.股票代码.apply(lambda x: "|" not in x)]]
    for i in range(1, maxam + 1):
        odf = df.loc[df.股票代码.apply(lambda x: "|" in x)]
        odf = odf.loc[odf[f"股票代码_{i}"].apply(lambda x: type(x) is str and len(x) > 1)]
        l.append(
            odf[yx(set(odf.columns) - {f"股票代码_{i1}" for i1 in range(1, maxam + 1) if i1 != i}, list(odf.columns))])
    return pd.concat(l).sort_values(by=["单位", "姓名"])


def splitrowsto1col(df, splits=10):
    pis = df.股票代码.apply(lambda x: len(x.split("|")))
    maxam = pis[pis < splits].max()
    for i in range(1, maxam + 1):
        df[f"股票代码_{i}"] = df.股票代码.apply(
            lambda x: yx(set(x.split("|")), x.split("|"))[i - 1] if len(x.split("|")) >= i else "")
    l = [df.loc[df.股票代码.apply(lambda x: "|" not in x)]]
    for i in range(1, maxam + 1):
        odf = df.loc[df.股票代码.apply(lambda x: "|" in x)]
        odf = odf.loc[odf[f"股票代码_{i}"].apply(lambda x: type(x) is str and len(x) > 1)]
        odf.股票代码 = odf[f"股票代码_{i}"]
        l.append(
            odf[yx(set(odf.columns) - {f"股票代码_{i1}" for i1 in range(1, maxam + 1) if i1 != i}, list(odf.columns))])

    return pd.concat(l)[df.columns].sort_values(by=["单位", "姓名"])
