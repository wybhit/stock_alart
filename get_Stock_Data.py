from datetime import datetime
import struct
import akshare as ak
import pandas as pd
import numpy as np
from get_Data_Tools import file_exist_or_get_data, file_exist_or_get_data_decorator
from get_Data_Tools import Get_Data_Tools


class Stock_A_History_Date:
    def __init__(self,days=365,market="A"):
        self.days = days
        #初始化日期等数据
        self.Data_Tools = Get_Data_Tools(market)
        self.LastTradeDate = self.Data_Tools.LastTradeDate

    @file_exist_or_get_data_decorator([1,"A"])
    def get_stock_zh_a_daily_hist(self,code):
        #读取config文件中的LastTradeDate
        end_date = self.LastTradeDate
        start_date=(datetime.now() - pd.Timedelta(days=self.days)).strftime('%Y%m%d')
        return ak.stock_zh_a_hist(symbol=code, period="daily",
                                start_date=start_date,
                                end_date=end_date,adjust="qfq")

    @file_exist_or_get_data_decorator([1,"A"])
    def get_history_max_price(self):
        # 获取所有A股股票代码
        stock_info = file_exist_or_get_data(ak.stock_info_a_code_name,[1,"A"])
        # 创建一个空的DataFrame来存储结果
        result_df = pd.DataFrame(columns=['股票代码', '股票名称', '历史最高日期','距今交易日数'])

        # 遍历每只股票
        for index, row in stock_info.iterrows():
            try:
                code = row['code']
                name = row['name']
                # 获取历史数据
                hist_data = self.get_stock_zh_a_daily_hist(code)
                # 计算最高价转化位数字
                hist_data['最高'] = hist_data['最高'].astype(float)
                max_price = hist_data['最高'].max()
                max_date = hist_data['日期'][hist_data['最高'] == max_price].iloc[-1]
                #是倒数第几行
                max_date_index = len(hist_data)-hist_data.index[hist_data['日期'] == max_date].tolist()[-1]+1

                # 添加到结果DataFrame
                result_df = pd.concat([result_df, pd.DataFrame({'股票代码': [code], '股票名称': [name],
                                                                '历史最高': [max_price],'历史最高日期': [max_date],
                                                                '距今交易日数': [max_date_index]})], ignore_index=True)

                print(f"已处理: {code} {name}")
            except Exception as e:
                print(f"处理{code}时出错: {str(e)}")
                continue
        return result_df


class Stock_A_Real_Time_Data:
    def __init__(self):
        self.Data_Tools = Get_Data_Tools()

    #获取全部A股实时股价数据
    def get_stock_zh_a_realtime(self):
        #通过akshare获取实时行情数据
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()

        return stock_zh_a_spot_em_df


if __name__ == "__main__":
    a = Stock_A_History_Date()
    a_df = a.get_history_max_price()
    b = Stock_A_Real_Time_Data()
    b_df = b.get_stock_zh_a_realtime()
    #合并a_df和b_df,以股票代码为key
    result_df = pd.merge(a_df, b_df, left_on='股票代码', right_on='代码', how='inner')
    result_df.to_csv("result_df.csv",index=False)

    
    result_df['历史最高'] = result_df['历史最高'].astype(float)
    result_df['最高'] = result_df['最高'].astype(float)
    result_df['流通市值'] = result_df['流通市值'].astype(int)
    
    
    #筛选最高价小于最新价，流通市值大于100亿
    result_df = result_df[result_df["历史最高"]<result_df["最高"]]
    result_df = result_df[result_df["流通市值"]>10000000000]
    #result_df重新编号,按流通市值由大到小排序
    result_df = result_df.sort_values(by="流通市值",ascending=False)
    result_df = result_df.reset_index(drop=True)
    result_df.to_csv("result_df.csv",index=False)



    print(result_df)



    

