# 这是一个示例 Python 脚本。
import math

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import requests,csv
import os
import json
import pandas as pd
import datetime
from collect_stock import getSenior, getCompany

original_keys = ["SECURITY_CODE", "SECUCODE", "ORG_NAME", "FOUND_DATE", "ADDRESS", "REG_ADDRESS", "ORG_WEB",
                 "ORG_TEL", "ORG_EMAIL", "EMP_NUM",
                 "BELONG_INDUSTRY", "ORG_EN_ABBR",
                 "SECRETARY", "CHAIRMAN",
                 "ORG_PROFILE"]
read_keys = ["股票代码", "SECUCODE", "公司名称", "成立日期", "办公地址", "注册地址", "公司网址",
             "办公电话", "公司邮箱", "雇员人数",
             "所属行业", "英文简称",
             "董秘", "董事长",
             "公司简介"]

def getHtml(url1):
    header1={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57',
        # 'Host': 'emweb.securities.eastmoney.com',
        'Accept': '* / *',
        'Accept-Encoding': 'gzip, deflate, br',
        # 'X-Requested-With': 'XMLHttpRequest',
        # 'Cookie': 'Hm_lvt_f5b8577eb864c9edb45975f456f5dc27=1685408317; st_si=14295653163321; st_asi=delete; qgqp_b_id=3d8cd7c6aea57e2dc73c4803fc882a75; xsb_history=430017%7C%u661F%u660A%u533B%u836F%2C830779%7C%u6B66%u6C49%u84DD%u7535; HAList=ty-0-301337-C%u4E9A%u534E%2Cty-1-688458-%u7F8E%u82AF%u665F%2Cty-0-001282-C%u4E09%u8054%u953B%2Cty-0-839719-%u5B81%u65B0%u65B0%u6750; st_pvi=98067314046262; st_sp=2023-05-30%2008%3A58%3A37; st_inirUrl=https%3A%2F%2Fdata.eastmoney.com%2Fxg%2Fxg%2F; st_sn=59; st_psi=20230530135430925-113301310291-8307670089; Hm_lpvt_f5b8577eb864c9edb45975f456f5dc27=1685426139',
    }
    response1 = requests.get(url1,header1)
    basic_data=json.loads(response1.text)
    # length=data['total']
    # print(basic_data)
    stock_datas = []
    if basic_data['code']==0 and 'data' in basic_data['result']:
        records_=basic_data['result']['data']
        for records in records_:
            stock_data = {}
            for o_k in original_keys:
                try:
                    stock_data[read_keys[original_keys.index(o_k)]] = records[o_k]
                except:
                    print("FALSE KEY: ", o_k, read_keys[original_keys.index(o_k)])
                    stock_data[read_keys[original_keys.index(o_k)]] = ""
            stock_datas.append(stock_data)
    return stock_datas


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    path = "D:/港股上市公司.xlsx"
    t1=datetime.datetime.now()
    # data=pd.read_excel("D:\上半年前五百强.xlsx",dtype=str)#,header=0,dtype=str,sheet_name=1)
    # length=len(data)
    # scode_list=data['股票代码']
    scode_list = list(range(1, 3999))
    stock_datas=[]; management_datas=[]
    for i in range(1, 12):#len(scode_list)):
        # print(scode_list[i])
        # if scode_list[i]:
        # url1 = "https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_INFO_ORGPROFILE&columns=SECUCODE%2CSECURITY_CODE%2CORG_CODE%2CSECURITY_INNER_CODE%2CORG_NAME%2CORG_EN_ABBR%2CBELONG_INDUSTRY%2CFOUND_DATE%2CCHAIRMAN%2CREG_PLACE%2CADDRESS%2CEMP_NUM%2CORG_TEL%2CORG_FAX%2CORG_EMAIL%2CORG_WEB%2CORG_PROFILE&quoteColumns=&filter=(SECUCODE%3D%22"+'YMM.N'+"%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=SECURITIES&client=PC&v=08324122111414072"
        url1 = f"https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_INFO_ORGPROFILE&columns=SECUCODE%2CSECURITY_CODE%2CORG_NAME%2CORG_EN_ABBR%2CBELONG_INDUSTRY%2CFOUND_DATE%2CCHAIRMAN%2CSECRETARY%2CACCOUNT_FIRM%2CREG_ADDRESS%2CADDRESS%2CYEAR_SETTLE_DAY%2CEMP_NUM%2CORG_TEL%2CORG_FAX%2CORG_EMAIL%2CORG_WEB%2CORG_PROFILE%2CREG_PLACE&quoteColumns=&pageNumber={i}&pageSize=500&sortTypes=&sortColumns=&source=F10&client=PC&v=020011544279406723"#&filter=(SECUCODE%3D%22"+f"{scode_list[i]:0>5d}"+".HK%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=F10&client=PC&v=020011544279406723"
        # url1 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code="+str(market_list[i])+str(scode_list[i])
        stock_datas += getHtml(url1)
        # break
    dataframe1 = pd.DataFrame(stock_datas)
    # dataframe2 = pd.DataFrame(management_datas)
    # if management_datas:
    #     dataframe2.columns = ["股票代码", "姓名", "性别", "年龄", "学历", "任职时间", "职务", "简历", "薪资（万元）"]
    while True:
        try:
            with (pd.ExcelWriter(path, mode='w', engine='openpyxl') if not os.path.exists(path) else
                  pd.ExcelWriter(path, mode='a', if_sheet_exists='new',
                                 engine='openpyxl') as writer):
                dataframe1.sort_values(by=["股票代码"]).drop_duplicates(subset=["公司名称"]).to_excel(writer, index=False, sheet_name="F10_去重")
                # dataframe2.to_excel(writer, index=False, sheet_name="高管")
            break
        except Exception as e:
            input('请关闭文件 (press Enter)')
    t2=datetime.datetime.now()
    print("采集用时：",(t2-t1).seconds,"秒")

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
