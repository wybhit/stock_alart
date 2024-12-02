from datetime import timezone
from pathlib import Path
from typing import Any, Optional, Dict, Union, List, Callable
import pandas as pd
import logging
from functools import wraps
# from config.config_manager import ConfigTools
from config.config_manager import ConfigTools

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

def create_filename(func_name: str, args: tuple, kwargs: dict, trade_date: str = "") -> str:
    """生成统一的文件名
    
    Args:
        func_name: 函数名
        args: 位置参数
        kwargs: 关键字参数
        trade_date: 交易日期，可选
        
    Returns:
        str: 生成的文件名
    """
    parts = [func_name]
    # 添加非类实例的参数到文件名
    parts.extend(str(arg) for arg in args if not str(arg).startswith("<"))
    # 添加关键字参数
    parts.extend(f"{k}_{v}" for k, v in kwargs.items())
    
    # 组合基础文件名
    base_name = "_".join(parts)
    
    # 如果提供了交易日期，则添加到文件名中
    if trade_date:
        return f"{base_name}_{trade_date}.csv"
    return base_name


def file_exist_or_get_data_decorator(is_daily_update: bool = True, market: str = "A"):
    """改进的文件缓存装饰器"""
    def decorator(func: Callable[..., Union[pd.DataFrame, Any]]) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Union[pd.DataFrame, Any]:
            if not is_daily_update:
                return func(*args, **kwargs)

            try:
                # 添加配置检查
                if not hasattr(ConfigTools, 'get_config'):
                    raise ImportError("ConfigTools 类未正确导入或缺少 get_config 方法")
                    
                DataPathManager.ensure_base_path()
                market_code = MARKET_CODES.get(market)
                if not market_code:
                    raise ValueError(f"不支持的市场类型: {market}")

                config = ConfigTools()
                trade_date = config.get_config("Running.Settings", f"LastTradeDate_{market_code}")
                if not trade_date:
                    raise ValueError("未能获取交易日期")

                # 生成文件名
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
                base_filename = create_filename(func.__name__, args, kwargs, "")  # 不包含日期的基础文件名
                pattern = f"{base_filename}*.csv"
                DataPathManager.clean_old_files(pattern)

                # 保存新数据
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

if __name__ == "__main__":
    try:
        # 删除或替换这段代码，因为 TradeDate 类未定义
        # data_tools = TradeDate()
        # print(f"当前交易日: {data_tools.get_trade_date()}")
        # print(f"最后交易日: {data_tools.get_last_trade_date()}")
        
        # 可以改为测试现有功能
        @file_exist_or_get_data_decorator()
        def test_func():
            return pd.DataFrame({'test': [1, 2, 3]})
            
        result = test_func()
        print("测试完成")
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
