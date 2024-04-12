# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import requests,csv
import os
import json
import pandas as pd
import datetime
import math
# from requests_html import HTMLSession
# s = HTMLSession()
# response = s.get(url)
# response.html.render()
def getHtml(url1):
    header1={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57',
        # 'Host': 'emweb.securities.eastmoney.com',
        'Accept': '* / *',
        'Accept-Encoding': 'gzip, deflate, br',
        # 'X-Requested-With': 'XMLHttpRequest',
        # 'Cookie': 'qgqp_b_id=3d8cd7c6aea57e2dc73c4803fc882a75; st_si=27984884105378; st_asi=delete; Hm_lvt_f5b8577eb864c9edb45975f456f5dc27=1685327600; HAList=ty-0-301232-%u98DE%u6C83%u79D1%u6280%2Cty-0-301337-C%u4E9A%u534E; st_pvi=03349661551168; st_sp=2023-05-29%2010%3A21%3A39; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=30; st_psi=2023052911034842-111000300841-4125258917; Hm_lpvt_f5b8577eb864c9edb45975f456f5dc27=1685347146',
    }
    response1 = requests.get(url1,header1)
    basic_data=json.loads(response1.text)
    # length=data['total']
    stock_datas = []
    if basic_data['code']==0 and 'result' in basic_data:
        # print(basic_data)
        records=basic_data['result']['data']
        # record=basic_data['result']['data'][0]
        for i in range(len(records)):
            record=records[i]
            if not record["PERSON_NAME"]:
                continue
            print(record)
            stock_data = []
            stock_data.append(record["SECUCODE"]) #股票代码
            stock_data.append(record["PERSON_NAME"]) #姓名
            stock_data.append(record["SEX"]) #性别
            if record["BRITH_YEAR"] is not None:
                stock_data.append(2023-record["BRITH_YEAR"]) #年龄
            else:
                stock_data.append("") #年龄

            # stock_data.append(record["HIGH_DEGREE"]) #学历
            # stock_data.append(records["HOLD_NUM"]) #HOLD_NUM
            stock_data.append(record["INCUMBENT_START_DATE"]) #任职时间
            # stock_data.append(records["INCUMBENT_START_DATE"]) #任职结束时间
            # stock_data.append(records["IS_COMPARE"]) #IS_COMPARE
            # stock_data.append(records["PERSON_CODE"]) #PERSON_CODE
            stock_data.append(record["POSITION_NAME"]) #职务
            # stock_data.append(records["POSITION_TYPE_CODE"]) #职务类型代码
            stock_data.append(record["RESUME"]) #简历
            stock_datas.append(stock_data)
            # if records["SALARY"] is not None:
            #     stock_data.append(round(records["SALARY"]/10000,4)) #薪资
            # else:
            #     stock_data.append("") #薪资

    # stock_data.append(records["SECUCODE"]) #股票代码+所属市场
    return stock_datas

# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    t1=datetime.datetime.now()
    path = 'D:\港股上市公司.xlsx'
    # data=pd.read_excel("D:\上半年前五百强.xlsx",header=0,dtype=str,sheet_name=1)
    # length=len(data)
    # market_list=data['TRADE_MARKET']
    # scode_list=data['SECURITY_CODE']
    # url_list=data['股票代码']

    stock_datas=[]
    for i in range(68):
        # print(scode_list[i])
        # url1 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code="+str(market_list[i])+str(scode_list[i])
        # RPT_USF10_BASIC_EXECUTIVEINFO
        # url1='https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_BASIC_EXECUTIVEINFO&columns=SECUCODE%2CSECURITY_CODE%2CPERSON_NAME%2CSEX%2CHIGH_DEGREE%2CBIRTH_YEAR%2CPOSITION_NAME%2CRESUME_EN%2CRESUME&quoteColumns=&filter=(SECUCODE%3D%22'+'YMM.N'+'%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=SECURITIES&client=PC&v=02675035649381523'
        url1=f'https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKPCF10_BASIC_EXECUTIVEINFO&columns=SECUCODE%2CSECURITY_NAME_ABBR%2CSECURITY_CODE%2CPERSON_NAME%2CPERSON_CODE%2CPOSITION_NAME%2CINCUMBENT_START_DATE%2CINCUMBENT_END_DATE%2CRESUME%2CSEX%2CBRITH_YEAR%2CNUM&quoteColumns=&filter=&pageNumber={i + 1}&pageSize=500&sortTypes=&sortColumns=&source=F10&client=PC&v=09819205825273318'#(SECUCODE%3D%22'+str(url_list[i])+'.HK%22)(NUM%3D1)(PERSON_NAME%3C%3E%22NULL%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=F10&client=PC&v=09819205825273318'
        # url1 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax?code="+str(url_list[i])
        # print(url1)
        stock_data=getHtml(url1)
        if stock_data:
            stock_datas.extend(stock_data)
    dataframe = pd.DataFrame(stock_datas)
    dataframe.columns = ["股票代码","姓名", "性别", "年龄","任职时间","职务","简历"]
    while True:
        try:
            with (pd.ExcelWriter(path, mode='w', engine='openpyxl') if not os.path.exists(path) else
                  pd.ExcelWriter(path, mode='a', if_sheet_exists='new',
                                 engine='openpyxl') as writer):
                dataframe.sort_values(by=["股票代码"]).dropna(subset=["姓名"]).drop_duplicates(subset=["股票代码", "姓名", "职务"]).to_excel(writer, index=False, sheet_name="高管")
                # dataframe2.to_excel(writer, index=False, sheet_name="高管")
            break
        except Exception as e:
            input('请关闭文件 (press Enter)')
    t2 = datetime.datetime.now()
    print("采集用时：",(t2-t1).seconds,"秒")

    # dataframe.to_excel('C:\\Users\Administrator\Desktop\财富网\IPO高管简介.xlsx', index=False)

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
