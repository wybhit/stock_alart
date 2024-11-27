from pandas_market_calendars import get_calendar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Dict, Union, List, Callable
import pandas as pd
import logging
from functools import wraps
import os
from utils import ConfigTools

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 市场代码映射
MARKET_CODES = {
    "A": "XSHG",
    "US": "NYSE",
    "HK": "HKG"
}

class DataPathManager:
    """数据路径管理类"""
    BASE_PATH = Path("D:/my_stock_data")

    @classmethod
    def ensure_base_path(cls) -> None:
        """确保基础路径存在"""
        cls.BASE_PATH.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_file_path(cls, filename: str) -> Path:
        """获取完整文件路径"""
        return cls.BASE_PATH / filename

    @classmethod
    def clean_old_files(cls, pattern: str) -> None:
        """清理匹配模式的旧文件"""
        for file in cls.BASE_PATH.glob(pattern):
            try:
                file.unlink()
                logger.debug(f"已删除旧文件: {file}")
            except Exception as e:
                logger.warning(f"删除文件失败 {file}: {e}")

def create_filename(func_name: str, args: tuple, kwargs: dict, trade_date: str) -> str:
    """生成统一的文件名"""
    parts = [func_name]
    parts.extend(str(arg) for arg in args if not str(arg).startswith("<__main__."))
    parts.extend(f"{k}_{v}" for k, v in kwargs.items())
    return f"{'_'.join(parts)}_{trade_date}.csv"

def file_exist_or_get_data_decorator(is_daily_update: bool = True, market: str = "A"):
    """改进的文件缓存装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            if not is_daily_update:
                return func(*args, **kwargs)

            try:
                DataPathManager.ensure_base_path()
                market_code = MARKET_CODES.get(market)
                if not market_code:
                    raise ValueError(f"不支持的市场类型: {market}")

                config = ConfigTools()
                trade_date = config.get_config("Running.Settings", f"LastTradeDate_{market_code}")
                if not trade_date:
                    raise ValueError("未能获取交易日期")

                filename = create_filename(func.__name__, args, kwargs, trade_date)
                file_path = DataPathManager.get_file_path(filename)

                if file_path.exists():
                    logger.debug(f"从缓存读取数据: {filename}")
                    return pd.read_csv(file_path, dtype=object)

                logger.info(f"获取新数据: {func.__name__}")
                df = func(*args, **kwargs)
                if df.empty:
                    raise ValueError("获取到的数据为空")

                # 清理旧文件并保存新数据
                pattern = f"{func.__name__}*.csv"
                DataPathManager.clean_old_files(pattern)
                df.to_csv(file_path, index=False)
                logger.info(f"数据已保存: {filename}")

                return df

            except Exception as e:
                logger.error(f"数据处理失败: {str(e)}")
                raise

        return wrapper
    return decorator

def file_exist_or_get_data(func: Callable, decorator_args: List = [1, "A"], *args: Any, **kwargs: Any) -> pd.DataFrame:
    """改进的文件缓存函数"""
    is_daily_update, market = decorator_args
    decorated_func = file_exist_or_get_data_decorator(is_daily_update, market)(func)
    return decorated_func(*args, **kwargs)

class Get_Data_Tools:
    """数据工具类"""
    def __init__(self, market: str = "A"):
        if market not in MARKET_CODES:
            raise ValueError(f"不支持的市场类型: {market}")
            
        self.market = MARKET_CODES[market]
        self.config = ConfigTools()
        self.last_trade_date = self.get_last_trade_date()

    def get_trade_date(self, range_days: int = 1) -> Optional[str]:
        """获取交易日期"""
        try:
            calendar = get_calendar(self.market)
            schedule = calendar.schedule(
                start_date=(datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d')
            )

            if schedule.empty:
                return None

            latest_date = schedule.index[-1]
            market_close = schedule.loc[latest_date, 'market_close']
            market_open = schedule.loc[latest_date, 'market_open']
            
            now = datetime.now(market_close.tz)
            
            if market_open < now < market_close:
                return market_close.strftime('%Y%m%d')
            return None

        except Exception as e:
            logger.error(f"获取交易日期失败: {str(e)}")
            return None

    def get_last_trade_date(self, range_days: int = 10) -> str:
        """获取最近的交易日期"""
        try:
            calendar = get_calendar(self.market)
            schedule = calendar.schedule(
                start_date=(datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d')
            )

            if schedule.empty:
                raise ValueError("未能获取交易日历")

            latest_close = schedule.loc[schedule.index[-1], 'market_close']
            now = datetime.now(latest_close.tz)

            # 确定最后交易日
            if now < latest_close:
                trade_date = schedule.loc[schedule.index[-2], 'market_close']
            else:
                trade_date = latest_close

            # 更新配置
            formatted_date = trade_date.strftime('%Y%m%d')
            self.config.set_config(
                "Running.Settings",
                f"LastTradeDate_{self.market}",
                formatted_date
            )

            return formatted_date

        except Exception as e:
            logger.error(f"获取最后交易日期失败: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        data_tools = Get_Data_Tools()
        print(f"当前交易日: {data_tools.get_trade_date()}")
        print(f"最后交易日: {data_tools.get_last_trade_date()}")
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
