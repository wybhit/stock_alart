from datetime import datetime
import akshare as ak
import pandas as pd
from typing import Optional, Dict, List
from pathlib import Path
import logging
from get_Data_Tools import file_exist_or_get_data, file_exist_or_get_data_decorator
from get_Data_Tools import Get_Data_Tools

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataProcessor:
    """股票数据处理基类"""
    def __init__(self, market: str = "A"):
        self.data_tools = Get_Data_Tools(market)
        self.last_trade_date = self.data_tools.last_trade_date

    def safe_convert_numeric(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """安全地转换数值类型"""
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

class StockAHistoryData(StockDataProcessor):
    """A股历史数据处理类"""
    def __init__(self, days: int = 365):
        super().__init__(market="A")
        self.days = max(1, days)  # 确保天数至少为1

    @file_exist_or_get_data_decorator(True, "A")
    def get_stock_daily_history(self, code: str) -> pd.DataFrame:
        """获取单个股票的历史数据"""
        try:
            start_date = (datetime.now() - pd.Timedelta(days=self.days)).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=self.last_trade_date,
                adjust="qfq"
            )
            if df.empty:
                raise ValueError(f"未获取到股票{code}的数据")
            return df
        except Exception as e:
            logger.error(f"获取股票{code}历史数据失败: {str(e)}")
            raise

    def process_single_stock(self, code: str, name: str) -> Optional[Dict]:
        """处理单个股票的数据"""
        try:
            hist_data = self.get_stock_daily_history(code)
            if hist_data.empty:
                return None

            hist_data = self.safe_convert_numeric(hist_data, ['最高'])
            hist_data = hist_data.dropna(subset=['最高'])

            if hist_data.empty:
                return None

            max_price_idx = hist_data['最高'].idxmax()
            max_date = hist_data.loc[max_price_idx, '日期']
            days_since_max = len(hist_data) - hist_data.index.get_loc(max_price_idx)

            return {
                '股票代码': code,
                '股票名称': name,
                '历史最高': float(hist_data.loc[max_price_idx, '最高']),
                '历史最高日期': str(max_date),
                '距今交易日数': int(days_since_max)
            }
        except Exception as e:
            logger.error(f"处理股票{code}数据时出错: {str(e)}")
            return None

    @file_exist_or_get_data_decorator(True, "A")
    def get_history_max_price(self) -> pd.DataFrame:
        """获取所有股票的历史最高价格数据"""
        try:
            stock_info = file_exist_or_get_data(ak.stock_info_a_code_name, [1, "A"])
            if stock_info.empty:
                raise ValueError("获取股票列表失败")

            results = []
            total_stocks = len(stock_info)

            for idx, row in stock_info.iterrows():
                logger.info(f"处理进度: {idx+1}/{total_stocks} - {row['code']} {row['name']}")
                result = self.process_single_stock(str(row['code']), str(row['name']))
                if result:
                    results.append(result)

            if not results:
                raise ValueError("未能获取任何有效数据")

            return pd.DataFrame(results)
        except Exception as e:
            logger.error(f"获取历史最高价格数据失败: {str(e)}")
            raise

class StockARealTimeData(StockDataProcessor):
    """A股实时数据处理类"""
    def get_realtime_data(self) -> pd.DataFrame:
        """获取实时行情数据"""
        try:
            df = ak.stock_zh_a_spot_em()
            if df.empty:
                raise ValueError("获取实时数据失败")
            return df
        except Exception as e:
            logger.error(f"获取实时行情数据失败: {str(e)}")
            raise

class StockDataAnalyzer:
    """股票数据分析类"""
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def process_and_analyze(self) -> pd.DataFrame:
        """处理和分析股票数据"""
        try:
            # 获取历史数据
            history_data = StockAHistoryData()
            max_price_df = history_data.get_history_max_price()

            # 获取实时数据
            realtime_data = StockARealTimeData()
            realtime_df = realtime_data.get_realtime_data()

            # 合并数据
            result_df = pd.merge(
                max_price_df,
                realtime_df,
                left_on='股票代码',
                right_on='代码',
                how='inner'
            )

            # 转换数据类型
            numeric_columns = ['历史最高', '最高', '流通市值']
            result_df = StockDataProcessor().safe_convert_numeric(result_df, numeric_columns)

            # 筛选数据
            filtered_df = result_df[
                (result_df['历史最高'] < result_df['最高']) &
                (result_df['流通市值'] > 1e10)
            ].copy()

            # 排序
            filtered_df = filtered_df.sort_values('流通市值', ascending=False).reset_index(drop=True)

            return filtered_df

        except Exception as e:
            logger.error(f"数据处理和分析失败: {str(e)}")
            raise

    def save_results(self, df: pd.DataFrame, filename: str = "result_df.csv") -> None:
        """保存分析结果"""
        try:
            output_file = self.output_dir / filename
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"结果已保存至: {output_file}")
        except Exception as e:
            logger.error(f"保存结果失败: {str(e)}")
            raise

def main():
    """主函数"""
    try:
        analyzer = StockDataAnalyzer()
        result_df = analyzer.process_and_analyze()
        analyzer.save_results(result_df)
        
        logger.info("\n筛选结果概览:")
        logger.info(f"\n{result_df}")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()



    

