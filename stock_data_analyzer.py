from datetime import datetime
import os
from typing import Optional, Dict, List

import akshare as ak
import pandas as pd
from tqdm import tqdm

from tools import ConfigTools
# from logger import setup_logger
from get_Data_Tools import file_exist_or_get_data, file_exist_or_get_data_decorator, Get_Data_Tools,file_cache_handler

logger = setup_logger('stock_analyzer')
config_tools = ConfigTools()

class StockHistoryAnalyzer:
    """股票历史数据分析器"""
    
    def __init__(self, days: int = None, market: str = "A") -> None:
        """
        初始化分析器
        
        Args:
            days: 历史数据天数
            market: 市场类型
        """
        self.days = days or config_tools.get_config("HISTORY_DAYS", 365)
        self.data_tools = Get_Data_Tools(market)
        self.last_trade_date = self.data_tools.LastTradeDate

    @file_cache_handler(is_daily_update=True, market="A")
    def get_stock_zh_a_daily_hist(self, code: str) -> pd.DataFrame:
        """获取单个股票的历史数据"""
        start_date = (datetime.now() - pd.Timedelta(days=self.days)).strftime('%Y%m%d')
        return ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=self.last_trade_date,
            adjust="qfq"
        )

    def get_history_max_price(self) -> pd.DataFrame:
        """获取所有股票的历史最高价信息"""
        # stock_info = file_exist_or_get_data(ak.stock_info_a_code_name, [1, "A"])
        stock_info = file_cache_handler(ak.stock_info_a_code_name, is_daily_update=True, market="A")
        result_list = []

        for _, row in tqdm(stock_info.iterrows(), total=len(stock_info), desc="处理股票数据"):
            try:
                code = row['code']
                hist_data = self.get_stock_zh_a_daily_hist(code)
                
                hist_data['最高'] = pd.to_numeric(hist_data['最高'], errors='coerce')
                max_price = hist_data['最高'].max()
                max_date_series = hist_data.loc[hist_data['最高'] == max_price, '日期']
                
                if max_date_series.empty:
                    continue
                    
                max_date = max_date_series.iloc[-1]
                max_date_index = len(hist_data) - hist_data.index[hist_data['日期'] == max_date].tolist()[-1]

                result_list.append({
                    '股票代码': code,
                    '股票名称': row['name'],
                    '历史最高': max_price,
                    '历史最高日期': max_date,
                    '距今交易日数': max_date_index
                })

            except Exception as e:
                logger.error(f"处理{code}时出错: {str(e)}")
                continue

        return pd.DataFrame(result_list)


class StockRealtimeDataProcessor:
    """实时股票数据处理器"""
    
    def __init__(self) -> None:
        self.data_tools = Get_Data_Tools()

    def get_stock_zh_a_realtime(self) -> pd.DataFrame:
        """获取A股实时数据"""
        return ak.stock_zh_a_spot_em()


def process_stock_data() -> Optional[pd.DataFrame]:
    """处理股票数据的主函数"""
    try:
        # 获取配置
        output_dir = config_tools.get_config("OUTPUT_DIR", "output")
        min_market_cap = config_tools.get_config("MIN_MARKET_CAP", 10_000_000_000)

        # 获取历史数据
        history_analyzer = StockHistoryAnalyzer()
        history_data = history_analyzer.get_history_max_price()
        logger.info("历史数据获取完成")

        # 获取实时数据
        realtime_processor = StockRealtimeDataProcessor()
        realtime_data = realtime_processor.get_stock_zh_a_realtime()
        logger.info("实时数据获取完成")

        # 合并数据
        result_df = pd.merge(
            history_data,
            realtime_data,
            left_on='股票代码',
            right_on='代码',
            how='inner'
        )

        # 转换数据类型
        numeric_columns = ['历史最高', '最高', '流通市值']
        for col in numeric_columns:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')

        # 筛选和排序
        filtered_df = (result_df
                      .query(f'历史最高 < 最高 and 流通市值 > {min_market_cap}')
                      .sort_values('流通市值', ascending=False)
                      .reset_index(drop=True))

        # 保存结果
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/stock_analysis_result.csv"
        filtered_df.to_csv(output_path, index=False)    
        logger.info(f"结果已保存到: {output_path}")

        return filtered_df

    except Exception as e:
        logger.error(f"数据处理出错: {str(e)}")
        return None


if __name__ == "__main__":
    result = process_stock_data()
    if result is not None:
        print("\n筛选结果预览：")
        print(result) 