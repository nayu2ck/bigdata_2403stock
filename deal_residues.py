# !/user/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import time
from datetime import datetime
import json
import re
import os
import numpy as np
from tools import *
from commons import *
import warnings
warnings.filterwarnings("ignore")

############
skip = 0
pth = "F:/0/0304/"
rfn = "人物信息管理-标签数据2"
want = ["企业", "非企业"]
out_cols = ['KID', 'ALUMNI_NAME', 'ALUMNI_LINK', 'KTAGS', 'INDUSTRY',
       'COMPANY_NAME', 'DUTY_NAME', '单位']
############

pthfl = f'{pth}结果3上市采集.xlsx'#f'{pth}{rfn}结果上市采集.xlsx'
pthrslt = f'{pth}{rfn}结果/'
pthorg = f'{pth}{rfn}/'

# ##

fake_names = ["中华人民共和国", "德国", "中国人", "日本", "韩国", "美国", "香港", "纽约", "东北", "渤海", "重庆市", "传媒公司", "集团公司"]  # 都有用
kwtrue = ["公司", "创投", "集团", "投资"]
kwfalse = ["局", "村", "委", "厅", "小学", "中学", "电视台", "县", "职务", "/", "退休", "人大", "政协"]  # "市", "区",
###@@
nz = set(ct + shi + gj + ss + ot + shen)


def seg_sub(t):
    try:
        t = re.search(
            '([^(\s|于|是|曾|年|任|原|局|校|学|会|\W)]|（|）|\(|\)){2,12}([学][院]|[大][学]|[医科信南北工][大]|[局]|[中][学]|[小][学]|[特][校]|[馆]|[\w]*[公][司]|[\w]*[集][团]|[电][视][台]|[银][行]|[科][技]|[会]|[村]|[厅]|[法医][院]|[所]|[\w]*[股][份]|[\w]*[投][资]|[\w]*[创][投]|[地][产]|[工农国民总分支][行])',
            t.replace(' ', '')).group()
        return t
    except:
        return False

def seg_etp(t):
    try:
        t = re.search(
            '([^(\s|于|是|曾|年|任|原|\W)]|（|）|\(|\)){2,12}([公][司]|[集][团]|[银][行]|[科][技]|[股][份]|[投][资]|[创][投]|[地][产]|[工农国民总分支][行])',
            t).group()
        return t
    except:
        return False


def wash_sub(t):
    x = seg_sub(t)
    return x if x else t


def filter_fs(in_df, do1=False, do2=False, do3=False):
    """
    和上市搜索程序中相同条件。筛掉。应该已采集过的数据
    :param in_df:
    :param do1: 学界政界OUT
    :param do2: 有行业值OUT
    :param do3: 单位为空out ...可选
    :return: 删减后的原df
    """
    in_df = in_df.loc[in_df.单位.apply(is_school).apply(qf)] if do1 else in_df
    in_df = in_df.loc[in_df["INDUSTRY"].isna()] if do2 else in_df
    if do3:
        # print("忽略了单位为空的数据")
        in_df.dropna(subset="单位", inplace=True)
        in_df = in_df.loc[in_df.单位.apply(lambda x: type(x) is str)]
    return in_df


def transf_sc(in_df, wash=False):
    """
    为和上市匹配后的表格一致，对单位列统一处理
    :param in_df:
    :param wash: 上市匹配的数据未进行这一处理(清洗过, 清洗方式有更新)
    :return: 对单位列处理后的原df
    """
    in_df["单位"] = in_df["单位"].apply(lambda x: spillt(x, [None, '，', '、', ','])[0] if not pd.isna(x) and re.search('[a-z|A-Z]', x) is None else x)
    if wash:
        in_df.单位 = in_df.单位.apply(wash_sub)
        print('重新提取了单位字段中的单位名称')
    return in_df


def save_data(write_path, output):
    while True:
        try:
            output[out_cols].to_excel(write_path, index=False)
            break
        except:
            input('关闭文件Enter')


if __name__ == '__main__':
    flag = pd.read_excel(pthfl, sheet_name=None)
    have = reach_files(pthrslt)
    have = list(map(lambda x: x.replace('-上市公司匹配.xlsx', ".xlsx").replace('-上市高管匹配.xlsx', ".xlsx"),
                    have))  # have.sort(key = lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))
    have = list(set(have))
    have.sort(key=lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))
    l = [] ; l2 = []
    sssum = 0
    rysum = 0
    sshz = pd.DataFrame()
    for hv in have[skip:]:
        dfo = filter_fs(pd.read_excel(pthorg + hv), 1, 1, 1)  # do1 do2 --去重复采集; do3 --去空白
        dfo.index = dfo["KID"]
        k = hv.replace(".xlsx", "")
        if k not in flag.keys():
            k = hv
        if k not in flag.keys():
            print(f"{hv}为空")
            continue
        flg = flag[k]#.replace(".xlsx", "")]
        sssum += len(flg)
        rysum += len(flg.loc[flg.人物已匹配])
        tad = dfo.loc[transf_sc(dfo).单位.apply(
            lambda x: x in fake_names or type(x) == str and len(re.findall('[a-z|A-Z|\s]', x)) > len(
                x) * .6)].KID.values.tolist()
        # 去除英文名和非单位名称
        rsd = dfo.loc[yx(set(dfo.index.to_list()) - (set(flg.KID) - set(tad)), dfo.index.to_list())]
        # 未匹配到人物，仍参与后续采集
        rsd2 = dfo.loc[yx(set(dfo.index.to_list()) - (set(flg.loc[flg.人物已匹配].KID) - set(tad)), dfo.index.to_list())]
        l.append(rsd)
        l2.append(rsd2)
        # rsdf = pd.read_excel(pthrslt+hv.replace(".xlsx", "-上市高管匹配.xlsx"))
        # rsdf.index = rsdf.KID
        # sshz = pd.concat([sshz, rsdf.loc[flg.KID]])
    # sshz.to_excel(pth + '上市汇总.xlsx', index=False)
    print(f"上市搜索程序匹配到单位{sssum}条，匹配到个人信息{rysum}条")
    df_rsd = pd.concat(l)
    df_rsd.单位 = df_rsd.单位.apply(wash_sub)
    print("原始表格", 5000 * len(have), "- 在上市表中有的 - 不用重复采集 - 空白", len(df_rsd), sep='\t')
    df_rsd2 = pd.concat(l2)
    print("+ 只匹配到单位未匹配到人名", len(df_rsd2),sep='\t')

    go = '0'#input("保留没有匹配到人名的记录？0/1")
    if go not in ['0', '']:
        ibool_select = list(map(lambda x: type(x) is str and ((seg_etp(x) or len(x) in range(2, 6)) and x not in nz and x[-1] not in ['会', '馆', '区']
                                                              and len([i for i in kwfalse if i in x]) == 0
                                                              or len([i for i in kwtrue if i in x]) > 0),
                                df_rsd2.单位.to_list()))
        bool_rsd = list(map(qf, ibool_select))
        df_rsd_非企业2 = df_rsd2.loc[bool_rsd]
        df_rsd_企业2 = df_rsd2.loc[ibool_select]
        for x in want:
            save_data(f"{pth}/{datetime.now().strftime('%m%d')}_{len(have)}fls_{x}pluspeof.xlsx", eval(f"df_rsd_{x}2"))

    ibool_select = list(map(lambda x: type(x) is str and ((seg_etp(x) or len(x) in range(2,6)) and x not in nz and x[-1] not in ['会', '馆']
                                                          and len([i for i in kwfalse if i in x]) == 0
                                                          or len([i for i in kwtrue if i in x]) > 0), df_rsd.单位.to_list()))
    bool_rsd = list(map(qf, ibool_select))
    df_rsd_非企业 = df_rsd.loc[bool_rsd]
    df_rsd_企业 = df_rsd.loc[ibool_select]

    for x in want:
        save_data(f"{pth}/{datetime.now().strftime('%m%d')}_{len(have)}fls_{x}.xlsx", eval(f"df_rsd_{x}"))

    if input("继续采集单位？0/1") not in ['0', '']:
        if go not in ['0', '']:
            仅单位名称 = df_rsd_企业2.单位
            仅单位名称.name = "企业名称"
            仅单位名称.drop_duplicates(inplace=True)
            仅单位名称.to_excel(f'{pth}人物库待采集单位pluspeof.xlsx', index=True, index_label="序号")
        仅单位名称 = df_rsd_企业.单位
        仅单位名称.name = "企业名称"
        仅单位名称.drop_duplicates(inplace=True)
        仅单位名称.to_excel(f'{pth}人物库待采集单位.xlsx', index=True, index_label="序号")
    if input("和之前采集名单进行对比去重？0/1") not in ['0', '']:
        df单位s = []
        oos = int(input("请输入已有表格数量"))
        for j in range(oos):
            df单位s.append(pd.read_excel(f"F:/0/天眼查/企业相关/数据存放/人物库待采集单位{j + 1 if j > 0 else ''}.xlsx"))
        df2 = pd.concat(df单位s)
        if go and (input("上市采集到中文的公司还需要采集吗？0/1") not in ['0', '']):
            df1 = df_rsd_企业2.单位
        else:
            df1 = df_rsd_企业.单位
        df1.name = "企业名称"
        df1.drop_duplicates(inplace=True)
        df2.index = df2.序号
        s1 = set(df1.index)
        s2 = set(df2.index)
        df新企业相关采集名单 = df1.loc[yx(s1 - s2, list(df1.index))].drop_duplicates()
        df新企业相关采集名单.to_excel(f'F:/0/天眼查/企业相关/数据存放/人物库待采集单位{oos + 1}.xlsx', index=True, index_label="序号")
        print(f"新增待采集企业{len(df新企业相关采集名单)}个")
    print("企业", len(df_rsd_企业), "非企业", len(df_rsd_非企业))
    if go not in ['0', '']:
        print("加上没有采集到高管的上市公司：")
        print("企业", len(df_rsd_企业2), "非企业", len(df_rsd_非企业2))
    print('采集到单位', len(sshz.单位.drop_duplicates()), '个')
    print('采集到高管', len(sshz.loc[sshz.简历.notna()].drop_duplicates(subset=["姓名", "简历"])), '人')
    print("done")
