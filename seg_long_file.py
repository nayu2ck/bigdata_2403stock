# !/user/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import os

################
in_pth = 'F:/0/0304/人物信息管理-标签数据.xlsx'
seg_len = 5000  # 需要分割成的数据长度
################


def segfile(in_pth, sort_columns=None):
    if sort_columns is None:
        sort_columns = []
    if __name__ != '__main__':
        print('executing segfile.py...')
    write_path = in_pth.replace('.xlsx', '')
    if not os.path.isdir(write_path):
        os.mkdir(write_path)
    in_dfs = pd.read_excel(in_pth, sheet_name=None)
    for sn in in_dfs.keys():
        in_df = in_dfs[sn]
        if sort_columns:
            in_df.sort_values(by=sort_columns, inplace=True)
        length = len(in_df)
        for j in range(length // seg_len + 1):
            dfsub = in_df.iloc[j * seg_len: (j + 1) * seg_len]
            dfsub.to_excel(f"{write_path}/{sn}-{j}.xlsx", index=False)
    if __name__ == '__main__':
        print("done")


if __name__ == '__main__':
    segfile(in_pth, ["单位"])
