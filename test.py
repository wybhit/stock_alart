from datetime import datetime
from typing import Optional, Union
import pandas_market_calendars as mcal
import pandas as pd

class TradeDateTools:
    """交易日期工具类"""
    
    def __init__(self, exchange: str = 'SSE'):
        """
        初始化交易日期工具
        
        Args:
            exchange: 交易所代码，默认为上交所(SSE)
                     支持: SSE(上交所), SZSE(深交所), HKEX(港交所)等
        """
        self.calendar = mcal.get_calendar(exchange)
        
    def is_trade_date(self, date: Union[str, datetime, pd.Timestamp]) -> bool:
        """
        判断是否为交易日
        
        Args:
            date: 日期，支持字符串('YYYY-MM-DD')、datetime或Timestamp类型
        """
        if isinstance(date, str):
            date = pd.Timestamp(date)
        schedule = self.calendar.schedule(start_date=date, end_date=date)
        return len(schedule) > 0
    
    def get_next_trade_date(self, date: Union[str, datetime, pd.Timestamp], n: int = 1) -> str:
        """
        获取之后第n个交易日
        
        Args:
            date: 起始日期，支持字符串('YYYY-MM-DD')、datetime或Timestamp类型
            n: 向后第n个交易日，默认为1
        """
        if isinstance(date, str):
            date = pd.Timestamp(date)
        # 获取未来30天的交易日历（通常足够找到下n个交易日）
        schedule = self.calendar.schedule(
            start_date=date,
            end_date=date + pd.Timedelta(days=30)
        )
        valid_dates = schedule.index
        # 如果当前日期不是交易日，从下一个交易日开始计算
        if not self.is_trade_date(date):
            target_idx = n - 1
        else:
            target_idx = n
        
        if target_idx < len(valid_dates):
            return valid_dates[target_idx].strftime('%Y-%m-%d')
        raise ValueError(f"Cannot find next {n} trading day(s)")
    
    def get_prev_trade_date(self, date: Union[str, datetime, pd.Timestamp], n: int = 1) -> str:
        """
        获取之前第n个交易日
        
        Args:
            date: 起始日期，支持字符串('YYYY-MM-DD')、datetime或Timestamp类型
            n: 向前第n个交易日，默认为1
        """
        if isinstance(date, str):
            date = pd.Timestamp(date)
        # 获取过去30天的交易日历
        schedule = self.calendar.schedule(
            start_date=date - pd.Timedelta(days=30),
            end_date=date
        )
        valid_dates = schedule.index
        # 如果当前日期不是交易日，从上一个交易日开始计算
        if not self.is_trade_date(date):
            target_idx = -n
        else:
            target_idx = -(n + 1)
            
        if abs(target_idx) <= len(valid_dates):
            return valid_dates[target_idx].strftime('%Y-%m-%d')
        raise ValueError(f"Cannot find previous {n} trading day(s)")
    
    def get_trading_dates(self, start_date: Union[str, datetime, pd.Timestamp],
                         end_date: Union[str, datetime, pd.Timestamp]) -> list:
        """
        获取指定时间范围内的所有交易日
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            list: 交易日列表，格式为['YYYY-MM-DD']
        """
        schedule = self.calendar.schedule(start_date=start_date, end_date=end_date)
        return [d.strftime('%Y-%m-%d') for d in schedule.index]
    

if __name__ == "__main__":
    trade_tools = TradeDateTools()

    # 判断是否为交易日
    print(trade_tools.is_trade_date('2024-01-01'))  # False (元旦)
    print(trade_tools.is_trade_date('2024-01-02'))  # True

    # 获取下一个交易日
    print(trade_tools.get_next_trade_date('2024-01-01'))  # '2024-01-02'

    # 获取上一个交易日
    print(trade_tools.get_prev_trade_date('2024-01-02'))  # '2023-12-29'

    # 获取日期范围内的所有交易日
    dates = trade_tools.get_trading_dates('2024-01-01', '2024-01-10')
    print(dates)  # ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08', '2024-01-09', '2024-01-10']

    # 使用其他交易所的日历
    hk_trade_tools = TradeDateTools('HKEX')
    print(hk_trade_tools.is_trade_date('2024-01-02'))
