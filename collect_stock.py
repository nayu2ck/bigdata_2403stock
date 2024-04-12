import os
import time

import requests,csv
import re
import json
import pandas as pd


try_codes = {
    "上证主板": list(range(600000, 604000)) + list(range(605000, 606000)),
    "上证科创版": list(range(688000, 689000)),
    "深证主板": list(range(0, 3817)),#4999
    "深证创业板": list(range(300000, 302000)),
    "北交所": list(range(830000, 840000)) + list(range(870000, 889999)),
    "上证B股": list(range(900900, 900999)),
    "深证B股": list(range(200000, 201872))
}
get_codes = {}
for k in try_codes:
    get_codes[k] = []

def getSenior(url1):
    header1={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57',
        'Host': 'emweb.securities.eastmoney.com',
        'Accept': '* / *',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': 'qgqp_b_id=3d8cd7c6aea57e2dc73c4803fc882a75; st_si=27984884105378; st_asi=delete; Hm_lvt_f5b8577eb864c9edb45975f456f5dc27=1685327600; HAList=ty-0-301232-%u98DE%u6C83%u79D1%u6280%2Cty-0-301337-C%u4E9A%u534E; st_pvi=03349661551168; st_sp=2023-05-29%2010%3A21%3A39; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=30; st_psi=2023052911034842-111000300841-4125258917; Hm_lpvt_f5b8577eb864c9edb45975f456f5dc27=1685347146',
    }
    for i in range(3):
        try:
            response1 = requests.get(url1, headers=header1, timeout=(1.0 + i * 2, 6.0 + i * 2))
            break
        except Exception as e:
            print(f'重试中{i + 1}/3'); time.sleep(2); time.sleep(2)
            if i == 2:
                return False
    basic_data=json.loads(response1.text)
    # length=data['total']
    stock_datas=[]
    if 'gglb' in basic_data:
        records=basic_data['gglb']
        # recode=basic_data['gglb'][0]
        for i in range(len(records)):
            stock_data = []
            recode=records[i]
            stock_data.append(str(recode["SECURITY_CODE"])) #股票代码
            stock_data.append(recode["PERSON_NAME"]) #姓名
            stock_data.append(recode["SEX"]) #性别
            stock_data.append(recode["AGE"]) #年龄
            stock_data.append(recode["HIGH_DEGREE"]) #学历
            # stock_data.append(records["HOLD_NUM"]) #HOLD_NUM
            stock_data.append(recode["INCUMBENT_TIME"]) #任职时间
            # stock_data.append(records["IS_COMPARE"]) #IS_COMPARE
            # stock_data.append(records["PERSON_CODE"]) #PERSON_CODE
            stock_data.append(recode["POSITION"]) #职务
            # stock_data.append(records["POSITION_TYPE_CODE"]) #职务类型代码
            stock_data.append(recode["RESUME"].strip() if recode["RESUME"] else recode["RESUME"]) #简历
            if recode["SALARY"] is not None:
                stock_data.append(round(recode["SALARY"]/10000,4)) #薪资
            else:
                stock_data.append("") #薪资
            stock_datas.append(stock_data)

    # stock_data.append(records["SECUCODE"]) #股票代码+所属市场
    return stock_datas
def getCompany(url1):
    header1={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57',
        'Host': 'emweb.securities.eastmoney.com',
        'Accept': '* / *',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': 'Hm_lvt_f5b8577eb864c9edb45975f456f5dc27=1685408317; st_si=14295653163321; st_asi=delete; qgqp_b_id=3d8cd7c6aea57e2dc73c4803fc882a75; xsb_history=430017%7C%u661F%u660A%u533B%u836F%2C830779%7C%u6B66%u6C49%u84DD%u7535; HAList=ty-0-301337-C%u4E9A%u534E%2Cty-1-688458-%u7F8E%u82AF%u665F%2Cty-0-001282-C%u4E09%u8054%u953B%2Cty-0-839719-%u5B81%u65B0%u65B0%u6750; st_pvi=98067314046262; st_sp=2023-05-30%2008%3A58%3A37; st_inirUrl=https%3A%2F%2Fdata.eastmoney.com%2Fxg%2Fxg%2F; st_sn=59; st_psi=20230530135430925-113301310291-8307670089; Hm_lpvt_f5b8577eb864c9edb45975f456f5dc27=1685426139',
    }
    for i in range(3):
        try:
            response1 = requests.get(url1, headers=header1, timeout=(1.0 + i * 2, 6.0 + i * 2))
            break
        except Exception as e:
            print(f'重试中{i + 1}/3')
            if i == 2:
                return False
    basic_data=json.loads(response1.text)
    # length=data['total']
    stock_data = {}
    original_keys = ["SECURITY_CODE", "ORG_NAME", "SECURITY_TYPE", "PROVINCE", "ADDRESS", "REG_ADDRESS", "ORG_WEB",
                     "ORG_TEL", "ORG_EMAIL", "EMP_NUM", "TATOLNUMBER",
                     "INDUSTRYCSRC1", "REG_CAPITAL", "SECURITY_NAME_ABBR",
                     "PRESIDENT", "LEGAL_PERSON", "SECRETARY", "CHAIRMAN", "SECPRESENT",
                     "BUSINESS_SCOPE", "ORG_PROFILE"]
    read_keys = ["股票代码", "公司名称", "证券类别", "省份", "办公地址", "注册地址", "公司网址",
                 "办公电话", "公司邮箱", "雇员人数", "管理人员人数",
                 "所属证监会行业", " 注册资本（万元）", "A股简称",
                 "总经理", "法人代表", "董秘", "董事长", "证券事务代表",
                 "经营范围", "公司简介"]
    original_keys2 = ["FOUND_DATE", "AFTER_ISSUE_PE", "DEC_SUMISSUEFEE", "ISSUE_PRICE",
                      "ISSUE_WAY", "NET_RAISE_FUNDS", "LISTING_DATE", "ONLINE_ISSUE_DATE", "ONLINE_ISSUE_LWR",
                      "TOTAL_FUNDS", "TOTAL_ISSUE_NUM", "PAR_VALUE"]
    read_keys2 = ["成立日期", "发行市营率（%）", "发行费用（万元）", "每股发行价（元）",
                  "发行方式", "募集资金净额（亿元）", "上市日期", "网上发行日期", "定价中签率（%）",
                  "发行总市值（亿元）", "发行量（万股）", "每股面值（元）"]
    if 'jbzl' in basic_data:
        global d1
        if not 正搜索code and basic_data['fxxg'][0]["LISTING_DATE"] and int(basic_data['fxxg'][0]["LISTING_DATE"][:4]) < int(d1[:4]):
            return False
        print(url1)
        records=basic_data['jbzl'][0]
        assert len(original_keys) == len(read_keys)  # 修改要读取的字段对应
        for o_k in original_keys:
            try:
                stock_data[read_keys[original_keys.index(o_k)]] = records[o_k]
            except:
                print("FALSE KEY: ", o_k, read_keys[original_keys.index(o_k)])
                stock_data[read_keys[original_keys.index(o_k)]] = ""

        records2 = basic_data['fxxg'][0]
        for o_k in original_keys2:
            try:
                if o_k == "DEC_SUMISSUEFEE":
                    stock_data[read_keys2[original_keys2.index(o_k)]] = round(records2["DEC_SUMISSUEFEE"] / 10000, 4)
                elif o_k == "NET_RAISE_FUNDS":
                    stock_data[read_keys2[original_keys2.index(o_k)]] = round(records2["NET_RAISE_FUNDS"] / 100000000, 4)
                elif o_k == "TOTAL_FUNDS":
                    stock_data[read_keys2[original_keys2.index(o_k)]] = round(records2["TOTAL_FUNDS"] / 100000000, 4)
                elif o_k == "TOTAL_ISSUE_NUM":
                    stock_data[read_keys2[original_keys2.index(o_k)]] = round(records2["TOTAL_ISSUE_NUM"] / 10000, 4)
                else:
                    stock_data[read_keys2[original_keys2.index(o_k)]] = records2[o_k]
            except:
                stock_data[read_keys2[original_keys2.index(o_k)]] = ""  # print("FALSE KEY: ", o_k, read_keys2[original_keys2.index(o_k)])

    return stock_data


def get_all_codes(typ, in_cod=None):
    """
    在股票代码的范围内依次搜寻有返回值的代码,
    根据arg设定采集公司/高管或同时采集（影响所需采集时间）
    :param typ: try_codes.keys()
    :return: ./已收集的股票代码.xlsx  公司信息.xlsx  高管信息.xlsx
    """
    global get_codes, maxlen, arg, leap, refrain, reflength
    pth1 = r"./已收集的股票代码.xlsx"
    pth2 = f"./公司信息.xlsx"
    pth3 = f"./高管信息.xlsx"
    ready_codes = []
    leap = leap and os.path.exists(f"./{typ}log.txt")and not refrain and in_cod is None
    df0 = pd.DataFrame()

    if os.path.exists(pth1):
        ready_codes = pd.read_excel(pth1, sheet_name=None)
        if typ in ready_codes:
            df0 = ready_codes[typ]
            if not leap:
                df = pd.DataFrame()
                if arg == 2 and "人员已采集" in df0.columns:  # df -> redy_codes是不需要重复采集的股票代码
                    df = df0.loc[df0["人员已采集"]]
                elif "公司已采集" in df0.columns:
                    df = df0.loc[df0["公司已采集"]]
                if len(df) == 0:
                    ready_codes = [df0["股票代码"].values[0] - 1]
                else:
                    ready_codes = df["股票代码"].values.tolist()
                if len(df) < len(df0):
                    maxlen = min(df0["股票代码"].values[-1] - df0["股票代码"].values[0] + 1, maxlen)  # 高管/企业采集了 企业/高管还没采集，采集到目前进度为止
        else:
            ready_codes = []
    if in_cod is not None:
        if ready_codes:
            tries = list(filter(lambda x: max(ready_codes) < x <= max(try_codes[typ]), in_cod.iloc[:, 0].values.tolist()))
        else:
            tries = list(filter(lambda x: min(try_codes[typ]) <= x <= max(try_codes[typ]), in_cod.iloc[:, 0].values.tolist()))
        if len(tries) == 0:  # 也可能不是从小到大
            tries = list(filter(lambda x: x not in ready_codes, in_cod.iloc[:, 0].values.tolist()))
    else:
        if leap:
            if os.path.exists(f"./{typ}log.txt"):
                with open(f"./{typ}log.txt", "r", encoding="utf-8") as f:
                    ready_codes = f.readlines()
                    ready_codes = list(map(lambda x: int(x.replace("\n", "")), ready_codes))
        if refrain:
            # tries = df0.loc[df.index[-1]:,"股票代码"].values.tolist()
            tries = list(filter(lambda x: x > max(ready_codes), df0["股票代码"].values.tolist())) if ready_codes else df0["股票代码"].values.tolist()
            # tries = df0["股票代码"].values.tolist()[reflength :]
            # reflength += maxlen
            refrain = len(tries) > maxlen
        else:
            tries = list(filter(lambda x: x > max(ready_codes), try_codes[typ])) if ready_codes else try_codes[typ]
            if maxlen >= len(tries):
                reflength = -1

    company_datas = []
    management_datas = []
    markets = {'SH': lambda x: '上证' in x, 'SZ': lambda x: '深证' in x, 'BJ': lambda x: '北交' in x}
    mc = list(filter(lambda x: markets[x](typ), list(markets.keys())))[0]  # 'SH' if '上证' in typ else ''

    ready_codes = []
    if not tries:
        reflength = -1
        return
    for code in tries[:maxlen]:
        t = time.time()
        if arg == 2:
            data = getSenior(f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code={mc}{code:0>6d}")
            if data:
                management_datas.extend(data)
                ready_codes.append(code)
                print(f"已采集{tries.index(code) + 1}/{maxlen}", end="")
            else:
                print(f"无法采集{mc}{code}", end="")
        else:
            data = getCompany(f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code={mc}{code:0>6d}")
            if data:
                company_datas.append(data)
                if arg != 3:
                    data2 = getSenior(f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code={mc}{code:0>6d}")
                    if data2 is not False:
                        management_datas.extend(data2)
                    else:
                        with open(f"./{typ}errlog.txt", "a", encoding="utf-8") as f:
                            f.write(f"{code}\n")
                if code not in ready_codes:
                    ready_codes.append(code)
                print(f"已采集{tries.index(code) + 1}/{maxlen}", end="")
            else:
                print(f"无法采集{mc}{code:0>6d}", end="")
        print(f"\t{time.time()-t:.5f}s")
    if (leap or not refrain) or tries and ready_codes[-1] != code or len(ready_codes) < maxlen * .85:
        with open(f"./{typ}log.txt", "a", encoding="utf-8") as f:
            f.write(f"{code}\n")
    if not ready_codes:
        time.sleep(1.5)
        return None
    get_codes[typ] = ready_codes
    while True:
        try:
            with (pd.ExcelWriter(pth1, mode='w', engine='openpyxl') if not os.path.exists(pth1) else
                  pd.ExcelWriter(pth1, mode='a', if_sheet_exists='replace',
                                 engine='openpyxl') as writer):
                df = pd.DataFrame(ready_codes, columns=["股票代码"])
                df["SECUCODE"] = df["股票代码"].apply(lambda x: mc + '{:0>6d}'.format(x))
                df["公司已采集"] = False
                df["人员已采集"] = False
                if len(df0) > 0:
                    df = pd.concat([df0.loc[df0["股票代码"].apply(lambda x: x in ready_codes)], df]).drop_duplicates(subset="SECUCODE")
                if arg != 3:
                    df["人员已采集"] = True
                if arg != 2:
                    df["公司已采集"] = True
                pd.concat([df0.loc[df0["股票代码"].apply(lambda x: x not in ready_codes)] if len(df0) > 0 else df0, df]).sort_values(by=["股票代码"]).to_excel(writer, index=False, sheet_name=typ)
            break
        except Exception as e:
            input('请关闭文件 (press Enter)')
    if company_datas and arg != 2:
        while True:
            try:
                with (pd.ExcelWriter(pth2, mode='w', engine='openpyxl') if not os.path.exists(pth2) else
                      pd.ExcelWriter(pth2, mode='a', if_sheet_exists='overlay',
                                     engine='openpyxl') as writer):
                    try:
                        df0 = pd.read_excel(pth2, sheet_name=typ).dropna(how="all")
                        rows = len(df0)+1 if len(df0) > 0 else 0
                    except:
                        rows = 0
                    df = pd.DataFrame(company_datas)
                    df.to_excel(writer, startrow=rows, header=rows==0, index=False, sheet_name=typ)
                break
            except Exception as e:
                input('请关闭文件 (press Enter)')
    if management_datas and arg != 3:
        while True:
            try:
                with (pd.ExcelWriter(pth3, mode='w', engine='openpyxl') if not os.path.exists(pth3) else
                      pd.ExcelWriter(pth3, mode='a', if_sheet_exists='overlay',
                                     engine='openpyxl') as writer):
                    try:
                        df0 = pd.read_excel(pth3, sheet_name=typ).dropna(how="all")
                        rows = len(df0)+1 if len(df0) > 0 else 0
                    except:
                        rows = 0
                    df = pd.DataFrame(management_datas, columns=["股票代码", "姓名", "性别", "年龄", "学历", "任职时间", "职务", "简历", "薪资（万元）"])
                    df.to_excel(writer, startrow=rows, header=rows==0, index=False, sheet_name=typ)
                break
            except Exception as e:
                print(e)
                input('请关闭文件 (press Enter)')



def getHtml(url, rettype=""):
    header={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57',
        'Host': 'datacenter-web.eastmoney.com',
        'Referer': 'https://data.eastmoney.com/xg/xg/?st=LISTING_DATE,SECURITY_CODE',
        'Cookie': 'qgqp_b_id=9e85dcb1ad13cd80774658187347c355; st_si=56737941071964; st_asi=delete; st_pvi=57761807506322; st_sp=2023-08-07%2014%3A13%3A55; st_inirUrl=; st_sn=3; st_psi=20230807142254553-113300300986-4003560583; JSESSIONID=59905FCDC9B2CAB32A3738DEEA0CBE38'
    }
    # 获取新股列表
    for i in range(3):
        try:
            response = requests.get(url,headers=header, timeout=(1.0, 6.0))
            break
        except Exception as e:
            print(f'重试中{i+1}/3')
            if i == 2:
                return False
    res = re.findall(r"(?<=\().+?(?=\);)", response.text)[0]
    data=json.loads(res)['result']['data'] if json.loads(res)['result'] is not None else []
    df=pd.DataFrame(data)

    # 获取新股对应高管信息
    length = len(df)
    if 'SECUCODE' in df.columns:
        market_list = [""] * len(df)
        scode_list = df['SECUCODE']
    elif "TRADE_MARKET" in df.columns:
        df["TRADE_MARKET"].replace({'北京证券交易所': 'BJ', '深圳证券交易所': 'SZ','上海证券交易所':'SH'},inplace=True)
        market_list = df['TRADE_MARKET']
        scode_list = df['SECURITY_CODE']
    # url_list=data['股票代码']
    management_datas = []
    # 获取新股对应公司信息
    company_datas = []
    for i in range(length):
        if rettype != "management":
            url2 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code=" + (str(scode_list[i]) if 'SECUCODE' in df.columns else
                                                                                                     str(market_list[i]) + str(scode_list[i]))
            stock_data2 = getCompany(url2)
            if stock_data2:
                company_datas.append(stock_data2)
            elif stock_data2 is bool:
                break

        if rettype != "company":
            url1 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code=" + (str(scode_list[i]) if 'SECUCODE' in df.columns else
                                                                                                         str(market_list[i]) + str(scode_list[i]))
            # url1 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code=SH600029"
            stock_data = getSenior(url1)
            if stock_data:
                management_datas.extend(stock_data)

    dataframe = pd.DataFrame(management_datas)
    if management_datas:
        dataframe.columns = ["股票代码", "姓名", "性别", "年龄", "学历", "任职时间", "职务", "简历", "薪资（万元）"]

    dataframe2 = pd.DataFrame(company_datas)
    # dataframe2.columns = ["股票代码", "公司名称", "证券类别", "省份", "办公地址", "注册地址", "所属证监会行业",
    #                      "注册资本（万元）", "A股简称", "成立日期", "发行市营率（%）", "发行费用（万元）", "每股发行价（元）",
    #                      "发行方式", "募集资金净额（亿元）", "上市日期", "网上发行日期", "定价中签率（%）",
    #                      "发行总市值（亿元）", "发行量（万股）", "每股面值（元）"]

    if rettype == "merge":
        merged_df = pd.merge(dataframe, dataframe2, on='股票代码')
        return merged_df
    elif rettype == "management":
        return dataframe
    elif rettype == "company":
        return dataframe2
    return {"高管": dataframe, "公司": dataframe2}
    # TRADE_MARKET	ISSUE_NUM	ONLINE_ISSUE_NUM	ONLINE_APPLY_UPPER	ISSUE_PRICE	LATELY_PRICE	LISTING_DATE	AFTER_ISSUE_PE	INDUSTRY_PE_NEW	MAIN_BUSINESS
    # 上市交易所代码	发行总数（万股）	网上发行	申购上限	发行价格	最新价	上市日期	发行市营率	行业市营率	主营业务


def main(date1,date2,stock_type='',path='', ret=''):
    '''
    覆盖Sheet: bool
    :param date1: 开始时间
    :param date2: 截止时间
    :param stock_type: 股票类型['沪市主板', '科创版', '深市主板', '创业板', '北交所']
    :param path: write_path
    :param ret: return_type
    :return: True
    '''
    global 覆盖Sheet
    pageSize="500" #一页显示多少条数据
    filter=''
    if stock_type=="沪市主板":
        filter='(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CST="0101")'
    elif stock_type=="科创板":
        filter='(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CST="0102")'
    elif stock_type=="深市主板":
        filter='(SECURITY_TYPE_CODE="058001001")(TRADE_MARKET_CST="0201")'
    elif stock_type == "创业板":
        filter = '(SECURITY_TYPE_CODE="058001001")(TRADE_MARKET_CST="0202")'
    elif stock_type == "北交所":
        # url="https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123042218289778478835_1691480062980&sortColumns=APPLY_DATE&sortTypes=-1&pageSize={}&pageNumber=1&columns=ALL&reportName=RPT_NEEQ_ISSUEINFO_LIST&quoteColumns=f14~01~SECURITY_CODE~SECURITY_NAME_ABBR%2Cf2~01~SECURITY_CODE%2Cf3~01~SECURITY_CODE%2CNEW_CHANGE_RATE~01~SECURITY_CODE&quoteType=0&filter=(LISTING_DATE>='{}')(LISTING_DATE<='{}')&source=NEEQSELECT&client=WEB".format(pageSize,date1,date2)
        url="https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123036321545908050656_1691483702000&sortColumns=SELECT_LISTING_DATE&sortTypes=-1&pageSize=500&pageNumber=1&columns=ALL&reportName=RPT_NEEQ_ISSUEINFO_LIST&quoteColumns=f14~01~SECURITY_CODE~SECURITY_NAME_ABBR%2Cf2~01~SECURITY_CODE%2Cf3~01~SECURITY_CODE%2CNEW_CHANGE_RATE~01~SECURITY_CODE&quoteType=0&source=NEEQSELECT&client=WEB"
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123026095888089218056_1685412368659&sortColumns=LISTING_DATE,SECURITY_CODE&sortTypes=-1,-1&pageSize={}&pageNumber=1&reportName=RPTA_APP_IPOAPPLY&columns=SECURITY_CODE,SECURITY_NAME,TRADE_MARKET_CODE,APPLY_CODE,TRADE_MARKET,MARKET_TYPE,ORG_TYPE,ISSUE_NUM,ONLINE_ISSUE_NUM,OFFLINE_PLACING_NUM,LISTING_DATE&filter=(LISTING_DATE>='{}')(LISTING_DATE<='{}'){}&source=WEB&client=WEB".format(pageSize,date1,date2,filter) if stock_type != "北交所" else url
    print(url)
    # url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123026095888089218056_1685412368659&sortColumns=LISTING_DATE,SECURITY_CODE&sortTypes=-1,-1&pageSize="+pageSize+"&pageNumber=1&reportName=RPTA_APP_IPOAPPLY&columns=SECURITY_CODE,SECURITY_NAME,TRADE_MARKET_CODE,APPLY_CODE,TRADE_MARKET,MARKET_TYPE,ORG_TYPE,ISSUE_NUM,ONLINE_ISSUE_NUM,OFFLINE_PLACING_NUM,LISTING_DATE&filter=(LISTING_DATE>='"+date1+"')(LISTING_DATE<='"+date2+"')(SECURITY_TYPE_CODE+in+(%22058001001%22%2C%22058001008%22))(TRADE_MARKET_CST%3D%220102%22)&source=WEB&client=WEB"
    # path=r'D:\新上市公司\try.xlsx'
    data=getHtml(url, rettype=ret)
    while True:
        try:
            with (pd.ExcelWriter(path, mode='w', engine='openpyxl') if not os.path.exists(path) else
                  pd.ExcelWriter(path, mode='a', if_sheet_exists='replace' if 覆盖Sheet else 'new', engine='openpyxl') as writer):
                if ret:
                    data.to_excel(writer, index=False, sheet_name=stock_type + ret.replace("management", "高管").replace("company", "公司").replace("merge", ""))
                else:
                    for k in data:
                        data[k].to_excel(writer, index=False, sheet_name=stock_type + k)
            break
        except Exception as e:
            input('请关闭文件 (press Enter)')


if __name__ == '__main__':
    rettyps = {"1": "merge", "2": "management", "3": "company"}
    ###############
    d1 = '2024-02-01'
    d2 = '2024-03-08'
    arg = 0  # 返回表格形式 1.公司表和人员表merge 2.仅人员 3.仅公司 0.(默认)分开返回
    正搜索code = True  # 搜索全部股票代码 / 搜索d1—d2新上市的公司
    覆盖Sheet = True  # (相同范围)是否直接覆盖上一次采集的结果(否则新建一张同名Sheet)
    maxlen = 500  # 本次采集多少条数据 (预计时间: 0.7s/条) get_all_codes()使用
    leap = False  # 从log文件标记的最新行数开始搜索 (循环采集后续会自动设置为True)
    refrain = True or arg in [2, 3]  # 已知哪些代码有效
    reflength = 0
    typ = '深证B股'  # 要采集的股票类型
    in_path = './深证B股.xlsx'  # 待采集的股票代码或 公司名称（未添加）
    ###############
    if 正搜索code:
        in_df = pd.read_excel(in_path, usecols="A", dtype=int)
        # get_all_codes('上证主板')
        for maxlen in [60] * 200:  # 定时保存
            get_all_codes(typ, in_df)
            print()
            leap = os.path.exists(f"./{typ}log.txt")
            if reflength == -1:
                break
    for typ in ['沪市主板', '深市主板', '科创版', '创业板','北交所']:
        main(d1, d2, stock_type=typ, path=f'D:\stock{d1}_{d2}.xlsx', ret=rettyps[str(arg)] if arg else "")
        # main(d1, d2, stock_type=typ, path=f'D:\stock{typ}{d1}_{d2}.xlsx', ret=rettyps[str(arg)] if arg else "")