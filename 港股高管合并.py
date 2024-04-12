import pandas as pd
df = pd.read_excel("D:\财富网高管采集未清洗\@未清洗-20220825.xlsx",sheet_name=1)
# 对职务列数据进行合并，其他列数据保留第一行
merged_df = df.groupby("id").apply(lambda x: pd.Series({
    "id": x["id"].iloc[0],
    "标签2": x["标签"].iloc[0],
    "职务2": ",".join(x["职务"].astype(str))
})).reset_index(drop=True)
merged_df = pd.merge(merged_df, df,how='left', on='id')
# 输出合并结果
with pd.ExcelWriter('D:\财富网高管采集未清洗\@未清洗-20220825_港股.xlsx', engine='xlsxwriter',
      engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
    merged_df.to_excel(writer,index=False)