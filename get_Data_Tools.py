import pandas as pd
import json
from pandas_market_calendars import get_calendar
from datetime import datetime
import os
import akshare as ak
from tools import ConfigTools

class Get_Data_Tools:
    def __init__(self,market = "A"):
        if market == "A":
            self.market = "XSHG"
        elif market == "US":
            self.market = "NYSE"
        elif market == "HK":
            self.market = "HKG"

        self.LastTradeDate = self.get_last_trade_date()
        #写入config文件
        
    
    def get_trade_date(self,range_days = 1):
        """
        range_days: 获取最近range_days天的交易日
        return: 如果在交易时间，返回交易日期，否则返回False
        """
        #获取A股最近一个交易日
        stock_calendar = get_calendar(self.market)
        schedule_time = stock_calendar.schedule(start_date=(datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y%m%d'),
                                        end_date=datetime.now().strftime('%Y%m%d'))
        ts_close = schedule_time.loc[schedule_time.index[-1],'market_close']
        ts_open = schedule_time.loc[schedule_time.index[-1],'market_open']
        now = datetime.now(ts_close.tz)

        #如果是开盘时间
        if now<ts_close and now>ts_open:
            return ts_close.to_pydatetime().strftime('%Y%m%d')
        else:
            return False


    def get_last_trade_date(self, range_days = 10):
        #获取A股最近一个交易日
        stock_calendar = get_calendar(self.market)
        #获取最近range_days天的交易日
        schedule_time = stock_calendar.schedule(start_date=(datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y%m%d'),
                                        end_date=datetime.now().strftime('%Y%m%d'))
        ts = schedule_time.loc[schedule_time.index[-1],'market_close']
        now = datetime.now(ts.tz)

        # 如果当前时间大于最后一个交易日收盘时间，则返回最后一个交易日，否则返回倒数第二个交易日
        if now<ts.to_pydatetime():
            ts = schedule_time.loc[schedule_time.index[-2],'market_close']
        
        ConfigTools.set_config("Running.Settings","LastTradeDate_"+self.market ,ts.to_pydatetime().strftime('%Y%m%d'))

        return ts.to_pydatetime().strftime('%Y%m%d')


def file_exist_or_get_data_decorator(decorator_args):
    """
    装饰器，用于查询存储文件是否存在，如果存在，则读取文件，否则获取数据并存储
    decorator_args[0]: 是否为日更新数据
    decorator_args[1]: 市场
    """
    def decorator(func):
        def wrapper(*args,**kwargs):
            file_path = "D:\\my_stock_data"
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            # 日更新数据为盘后更新数据
            if decorator_args[0]:
                #
                if decorator_args[1] == "A":
                    market = "XSHG"
                elif decorator_args[1] == "US":
                    market = "NYSE"
                elif decorator_args[1] == "HK":
                    market = "HKG"
                trade_end_date = ConfigTools.get_config("Running.Settings","LastTradeDate_"+market)

                # 生成文件名
                file_name = func.__name__
                for arg in args:
                    #不是以"<__main__." 开头
                    if not str(arg).startswith("<__main__."):
                        file_name = file_name+ str(arg)
                for key,value in kwargs.items():
                    file_name = file_name+ str(key)+ "_"+ str(value)
                file_name_add_date = file_name + "_"+ trade_end_date+ ".csv"
                # 查询文件是否存在
                try:
                    df = pd.read_csv(os.path.join(file_path,file_name_add_date),dtype=object)
                except:
                    df = func(*args, **kwargs)
                    #删除含有func.__name__的文件
                    for file in os.listdir(file_path):
                        if file_name in file:
                            os.remove(os.path.join(file_path,file))
                    df.to_csv(os.path.join(file_path,file_name_add_date),index=False)
                return df
        return wrapper
    return decorator
    
def file_exist_or_get_data(func,decorator_args=[1,"A"],*args,**kwargs):
    """
    用于查询存储文件是否存在，如果存在，则读取文件，否则获取数据并存储
    decorator_args[0]: 是否为日更新数据
    decorator_args[1]: 市场
    """
    file_path = "D:\\my_stock_data"

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # 日更新数据为盘后更新数据
    if decorator_args[0]:
        #判断是否为收盘后时间
        if decorator_args[1] == "A":
            market = "XSHG"
        elif decorator_args[1] == "US":
            market = "NYSE"
        elif decorator_args[1] == "HK":
            market = "HKG"
            
        trade_end_date = ConfigTools.get_config("Running.Settings","LastTradeDate_"+market)

        # 生成文件名
        file_name = func.__name__
        for arg in args:
                #不是以"<__main__." 开头
                if not str(arg).startswith("<__main__."):
                    file_name = file_name+ str(arg)    
        for key,value in kwargs.items():
            file_name = file_name+ str(key)+ "_"+ str(value)
        file_name_add_date = file_name + "_"+ trade_end_date+ ".csv"

        # 查询文件是否存在
        try:
            df = pd.read_csv(os.path.join(file_path,file_name_add_date),dtype=object)
        except:
            df = func(*args, **kwargs)
            #删除含有func.__name__的文件
            for file in os.listdir(file_path):
                if file_name in file:
                    os.remove(os.path.join(file_path,file))
            df.to_csv(os.path.join(file_path,file_name_add_date),index=False)
        return df



if __name__ == "__main__":
    a = Get_Data_Tools()
    # print(file_exist_or_get_data(ak.stock_info_a_code_name))

    print(a.get_trade_date())
    print(a.get_last_trade_date())
    # print(get_stock_zh_a_daily_hist("000001"))

