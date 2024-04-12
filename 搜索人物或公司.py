# !/user/bin/env python3
# -*- coding: utf-8 -*-
import re

import pandas as pd
import os, traceback
from tools import *
from commons import *
from datetime import datetime
df_company = pd.read_excel('./公司信息.xlsx', sheet_name=None)
df_people = pd.read_excel('./高管信息.xlsx', sheet_name=None)
df_hk = pd.read_excel('F:/0/0304/港股上市公司.xlsx', sheet_name=None)
for k in df_company.keys():
    df_company[k].drop_duplicates(subset=["股票代码", "公司名称"], inplace=True)

sn_hk = {'company': '公司_去名称重复', 'people': '高管'}
cols_people = list(list(df_people.values())[0].columns)
cols_people_hk = list(df_hk["高管"].columns)

nz = set(ct + shi + gj + ss + ot + shen)

###########
in_df_path = 'F:/0/0304/人物信息管理-标签数据2.xlsx'
map_cols = {"ALUMNI_NAME": "姓名", "DUTY_NAME": "职位"}
###########

def search_company(in_df, write_path):
    output = pd.DataFrame(in_df)
    count = 0
    cmp_list = list(set(in_df["单位"].values.tolist()) - nz)
    for etprs in cmp_list:
        count += 1
        if type(etprs) != str or len(etprs) < 2:
            continue
        checknamegn = lambda x: sum(pd.isna([etprs, x])) == 0 and (etprs in x or x in etprs or len(set(etprs) & set(x)) >= min(len(set(x) - set('董事经理创始人')), len(set(etprs))))
        checkname = lambda x: sum(pd.isna([etprs, x])) == 0 and (etprs in x or x in etprs or (len(etprs) == 4 and etprs[2:] in x and etprs[:2] in x) and etprs[:2] not in nz)
        mada = False
        for k in list(df_company.keys()) + ['hk']:
            dfc = df_hk[sn_hk['company']] if k == 'hk' else df_company[k]
            mtc = dfc.loc[dfc['公司名称'].apply(checkname)]
            if not len(mtc) and '股票代码' in output.columns and pd.isna(output.loc[output['单位'] == etprs, '股票代码']).any(): #精确匹配不到且目前又无结果
                mtc = dfc.loc[dfc['公司名称'].apply(checknamegn)] #####粗略匹配
                mada = len(mtc) > 0 #等待继续精确匹配
            ovr = mada and len(list(filter(checkname, mtc.公司名称.to_list())))
            if len(mtc):
                print(f"比对中 {count}/{len(cmp_list)} " + etprs, mtc, sep='\n', end='\n\n')
                if '股票代码' in output.columns and not pd.isna(output.loc[output['单位'] == etprs, '股票代码']).any():
                    if not ovr:  # 没有在等待精确匹配/ 后面n次匹配也是粗略匹配
                        with (pd.ExcelWriter('/'.join(write_path.split('/')[:-1]) + f'多个证券类型.xlsx', mode='w', engine='openpyxl') if not os.path.exists('/'.join(write_path.split('/')[:-1]) + f'多个证券类型.xlsx') else
                              pd.ExcelWriter('/'.join(write_path.split('/')[:-1]) + f'多个证券类型.xlsx', mode='a', if_sheet_exists='overlay',
                                             engine='openpyxl') as writer):
                            try:
                                df0 = pd.read_excel('/'.join(write_path.split('/')[:-1]) + f'多个证券类型.xlsx', sheet_name=write_path.split("/")[-1]).dropna(how="all")
                                rows = len(df0) + 1 if len(df0) > 0 else 0
                            except:
                                rows = 0
                            df = pd.DataFrame([[etprs, '|'.join([f"{x:0>6d}" if type(x) != str else x for x in mtc['股票代码']]), k]], columns=["单位", "股票代码", "证券板块"])
                            df.to_excel(writer, startrow=rows, header=rows == 0, index=False, sheet_name=write_path.split("/")[-1])
                            continue
                    mada = False # overwrite
                if len(mtc) > 1:
                    with open('/'.join(write_path.split('/')[:-1]) + f'log.txt', 'a', encoding="utf-8") as f:
                        f.write(f'{write_path.split("/")[-1]} {etprs} {mtc["公司名称"].values.tolist()}\n{mtc["股票代码"].values.tolist()}\n\n')
                output.loc[output['单位'] == etprs, '股票代码'] = '|'.join([f"{x:0>6d}" if type(x) != str else x for x in mtc['股票代码']])
                output.loc[output['单位'] == etprs, '证券板块'] = k
    for i in range(3):
        try:
            if '/' in write_path and not os.path.isdir('/'.join(write_path.split('/')[:-1])):
                os.mkdir('/'.join(write_path.split('/')[:-1]))
            output.to_excel(write_path, index=False)
            break
        except:
            input('请关闭文件')
    return output

def search_people(in_df, write_path, w=False):
    if "股票代码" not in in_df.columns:
        print("输入错误数据")
        return False
    output = pd.DataFrame(in_df)
    ids = output.loc[output["股票代码"].notna()].index
    got_company = in_df.loc[ids, "KID"].drop_duplicates()
    got_people = []
    for i in ids:
        codes = in_df.loc[i, "股票代码"]
        codes = codes.split('|') if type(codes) is str else codes
        for code in codes:
            k = in_df.loc[i, "证券板块"]
            if k == 'hk':
                mtc = df_hk[sn_hk['people']].loc[df_hk[sn_hk['people']]["股票代码"] == code + ".HK"]
            else:
                mtc = df_people[k].loc[df_people[k]["股票代码"].apply(lambda x: f"{x:0>6d}" == code)]
            mtc = mtc.loc[mtc["姓名"] == in_df.loc[i, "姓名"]]
            if not len(mtc):
                continue
            print(in_df.loc[i, ["姓名", "单位", "职位"]], mtc, sep='\n', end='\n\n')
            got_people.append(in_df.loc[i, "KID"])
            for col in yx(set(cols_people_hk if k == 'hk' else cols_people) - set(in_df.columns), cols_people_hk if k == 'hk' else cols_people):
                vals = output.loc[i, col].split('|') if col in output.columns and not pd.isna(output.loc[i, col]) else []
                vals += set(mtc[col].apply(str))
                output.loc[i, col] = '|'.join(yx(set(vals) - {'nan'}, vals))
                output.loc[i, '股票代码FD'] = code#mtc.iloc[0,:].股票代码
    if __name__ == '__main__' or w:
        for i in range(3):
            try:
                output.to_excel(write_path, index=False)
                with (pd.ExcelWriter('/'.join(write_path.split('/')[:-1]) + f'上市采集.xlsx', mode='w',
                                     engine='openpyxl') if not os.path.exists(
                        '/'.join(write_path.split('/')[:-1]) + f'上市采集.xlsx') else
                      pd.ExcelWriter('/'.join(write_path.split('/')[:-1]) + f'上市采集.xlsx', mode='a',
                                     if_sheet_exists='replace',
                                     engine='openpyxl') as writer):
                    try:
                        # df0 = pd.read_excel('/'.join(write_path.split('/')[:-1]) + f'上市采集.xlsx',
                        #                     sheet_name=write_path.split("/")[-1]).dropna(how="all")
                        rows = 0  # len(df0) + 1 if len(df0) > 0 else 0
                    except:
                        rows = 0
                    df = pd.DataFrame(got_company, columns=["KID"])
                    df["单位已匹配"] = True
                    df["人物已匹配"] = False
                    df.loc[df["KID"].apply(lambda x: x in got_people), "人物已匹配"] = True
                    df.to_excel(writer, startrow=rows, header=rows == 0, index=False, sheet_name='-'.join(write_path.split("/")[-1].split('-')[:-1]))
                break
            except:
                input('请关闭文件')
    if __name__ != '__main__':
        return output, got_people
    return output


if __name__ == '__main__':
    if not os.path.isdir(in_df_path.replace('.xlsx', '')):
        from seg_long_file import segfile
        segfile(in_df_path)
    fns = reach_files(in_df_path.replace('.xlsx', ''))
    fns.sort(key=lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))
    have = reach_files(in_df_path.replace('.xlsx', f"结果/"))
    have = list(map(lambda x: x.replace('-上市公司匹配.xlsx', ".xlsx").replace('-上市高管匹配.xlsx', ".xlsx"), have)) #have.sort(key = lambda x: (int(spillt(x, 't-')[1]), int(spillt(x, '.-')[1])))
    for fn in list(filter(lambda x:'Sheet0' in x and int(re.search('[\d]+', x.replace('Sheet0', '')).group()) <= 150,fns)):#yx(set(fns) - set(have), fns):
        try:
            outdir = in_df_path.replace('.xlsx', f"结果/{fn}")
            in_df = pd.read_excel(in_df_path.replace('.xlsx', '/') + fn)
            in_df.dropna(subset="单位", inplace=True)
            in_df = in_df.loc[in_df.单位.apply(is_school).apply(qf)]
            in_df = in_df.loc[in_df["INDUSTRY"].isna()]
            in_df["单位"] = in_df["单位"].apply(lambda x: spillt(x, [None, '，', '、', ','])[0] if type(x)==str and re.search('[a-z|A-Z]', x) is None else x)
            zj = search_company(in_df, outdir.replace('.xlsx', '-上市公司匹配.xlsx'))
            for col in map_cols:
                if col in zj.columns:
                    zj[map_cols[col]] = zj[col]
            search_people(zj, outdir.replace('.xlsx', '-上市高管匹配.xlsx'))
        except:
            with open('./搜索人物或公司log.txt', 'a', encoding="utf-8") as f:
                f.write(
                    f"{fn} {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M ')}\n{traceback.format_exc()}\n\n")

    print('done')
    os.system('shutdown -s -f -t 5')

