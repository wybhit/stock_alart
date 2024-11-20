import struct
import pandas as pd
import os
import numpy as np
from datetime import datetime
from pandas_market_calendars import get_calendar

import akshare as ak

def get_max_price_250d():
    # 获取所有A股股票代码
    stock_info = ak.stock_info_a_code_name()
    
    # 创建一个空的DataFrame来存储结果
    result_df = pd.DataFrame(columns=['股票代码', '股票名称', '250日最高价'])
    
    # 遍历每只股票
    for index, row in stock_info.iterrows():
        try:
            code = row['code']
            name = row['name']
            
            # 获取历史数据
            hist_data = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                         start_date=(datetime.now() - pd.Timedelta(days=250)).strftime('%Y%m%d'),
                                         end_date=datetime.now().strftime('%Y%m%d'),adjust="qfq")
            
            # 计算最高价
            max_price = hist_data['最高'].max()
            
            # 添加到结果DataFrame
            result_df = pd.concat([result_df, pd.DataFrame({'股票代码': [code], '股票名称': [name], '250日最高价': [max_price]})], ignore_index=True)

            
            print(f"已处理: {code} {name}")
            
        except Exception as e:
            print(f"处理{code}时出错: {str(e)}")
            continue
    
    return result_df

def file_exist_or_get_data_decorator(decorator_args):
    """
    装饰器，用于查询存储文件是否存在，如果存在，则读取文件，否则获取数据并存储
    decorator_args[0]: 是否为日更新数据
    """
    def decorator(func):
        def wrapper(*args,**kwargs):
            
            file_path = "D:\\my_stock_data"
            
            
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            # 日更新数据为盘后更新数据
            if decorator_args[0]:
                #判断是否为收盘后时间
                if datetime.now().hour > 15:
                    trade_end_date = datetime.now().strftime('%Y%m%d')
                else:
                    trade_end_date=(datetime.now() - pd.Timedelta(days=1)).strftime('%Y%m%d')

                # 生成文件名
                file_name = func.__name__
                for arg in args:
                    file_name = file_name+ str(arg)
                file_name = file_name + "_"+ trade_end_date+ ".csv"


                # 查询文件是否存在
                if os.path.exists(os.path.join(file_path,file_name)):
                    df = pd.read_csv(os.path.join(file_path,file_name))
                else:
                    df = func(*args, **kwargs)
                    #删除含有func.__name__的文件
                    for file in os.listdir(file_path):
                        if func.__name__ in file:
                            os.remove(os.path.join(file_path,file))
                    df.to_csv(os.path.join(file_path,file_name),index=False)
                return df
        return wrapper
    return decorator

@file_exist_or_get_data_decorator([1])
def stock_info_a_code_name():
    # 获取所有A股股票代码
    stock_info = ak.stock_info_a_code_name() 
    # 创建一个空的DataFrame来存储结果
    return stock_info


def get_last_trade_date(market = "A", range_days = 5):
    #获取A股最近一个交易日
    if market == "A":
        keyWord = "XSHG"
    elif market == "US":
        keyWord = "NYSE"
    elif market == "HK":
        keyWord = "HKG"
    stock_calendar = get_calendar(keyWord)
    #判断是否为开盘时间
    schedule_time = stock_calendar.schedule(start_date=(datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y-%m-%d'), 
                                    end_date=datetime.now().strftime('%Y-%m-%d'))
    ts = schedule_time.loc[schedule_time.index[-1],'market_close']
    now = datetime.now(ts.tz)


    if now>ts.to_pydatetime():
        return ts.to_pydatetime().strftime('%Y-%m-%d')
    else:
        ts = schedule_time.loc[schedule_time.index[-2],'market_close']
        return ts.to_pydatetime().strftime('%Y-%m-%d')


if __name__ == "__main__":
    print(get_last_trade_date())


    
