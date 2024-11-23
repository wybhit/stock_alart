import struct
import pandas as pd
import numpy as np
from datetime import datetime
import json
import akshare as ak
from get_Data_Tools import file_exist_or_get_data,Get_Data_Tools,file_exist_or_get_data_decorator

from tools import ConfigTools

class Stock_A_History_Date:
    def __init__(self,days=365):
        self.days = days
        self.Data_Tools = Get_Data_Tools()


    @file_exist_or_get_data_decorator([1])
    def get_stock_zh_a_daily_hist(self,code):
        #读取config文件中的LastTradeDate
        end_date = ConfigTools.get_config("Running.Settings","LastTradeDate")
        start_date=(datetime.now() - pd.Timedelta(days=self.days)).strftime('%Y%m%d')
        return ak.stock_zh_a_hist(symbol=code, period="daily", 
                                start_date=start_date,
                                end_date=end_date,adjust="qfq")
    
    @file_exist_or_get_data_decorator([1])
    def get_history_max_price(self):
        # 获取所有A股股票代码
        stock_info = file_exist_or_get_data(ak.stock_info_a_code_name,[1])
        # 创建一个空的DataFrame来存储结果
        result_df = pd.DataFrame(columns=['股票代码', '股票名称', '250日最高价'])
        
        # 遍历每只股票
        for index, row in stock_info.iterrows():
            try:
                code = row['code']
                name = row['name']
                
                # 获取历史数据
                hist_data = self.get_stock_zh_a_daily_hist(code)
                
                # 计算最高价
                max_price = hist_data['最高'].max()
                
                # 添加到结果DataFrame
                result_df = pd.concat([result_df, pd.DataFrame({'股票代码': [code], '股票名称': [name], '250日最高价': [max_price]})], ignore_index=True)

                
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
    stock_a_history_date = Stock_A_Real_Time_Data()
    print(stock_a_history_date.get_stock_zh_a_realtime())


    

