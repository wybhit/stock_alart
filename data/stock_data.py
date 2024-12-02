from pathlib import Path
from typing import Dict, List, Optional

import akshare as ak
from config.config_manager import ConfigTools
from data.tools import MARKET_CODES, file_exist_or_get_data, file_exist_or_get_data_decorator, logger


import pandas as pd
from pandas_market_calendars import get_calendar


from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class TradeDateTools:
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
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - pd.Timedelta(days=range_days)).strftime('%Y%m%d')
            
            schedule = calendar.schedule(start_date=start_date, end_date=end_date)
            
            if schedule.empty:
                raise ValueError("未能获取交易日历")
            
            # 获取最后一个交易日的收盘时间
            latest_date = schedule.index[-1]
            latest_close = schedule.loc[latest_date, 'market_close']
            now = datetime.now(latest_close.tz)
            
            # 如果当前时间早于最后一个交易日的收盘时间，则取前一个交易日
            if now < latest_close and len(schedule) > 1:
                trade_date = schedule.iloc[-2]['market_close']
            else:
                trade_date = latest_close
                
            formatted_date = trade_date.strftime('%Y%m%d')
            self.config.set_config("Running.Settings", f"LastTradeDate_{self.market}", formatted_date)
            
            return formatted_date
            
        except Exception as e:
            logger.error(f"获取最后交易日期失败: {str(e)}")
            raise


class StockDataProcessor:
    """股票数据处理基类"""
    def __init__(self, market: str = "A"):
        self.data_tools = TradeDateTools(market)
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
        try:
            hist_data = self.get_stock_daily_history(code)
            if hist_data is None or hist_data.empty:
                logger.warning(f"股票 {code} {name} 未获取到历史数据")
                return None

            # 确保列名存在
            required_columns = ['日期', '最高']
            if not all(col in hist_data.columns for col in required_columns):
                logger.warning(f"股票 {code} {name} 数据格式不正确")
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
            stock_list = file_exist_or_get_data(ak.stock_info_a_code_name, [1, "A"])
            if stock_list.empty:
                raise ValueError("获取股票列表失败")

            results = []
            total_stocks = len(stock_list)
            processed_count = 0
            error_count = 0
            
            # 计算最佳线程数
            max_workers = min(20, (total_stocks + 49) // 50)  # 每50个股票分配一个线程，最多20个线程
            
            # 使用线程池处理数据
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 分批提交任务，避免内存占用过大
                batch_size = 100
                for i in range(0, total_stocks, batch_size):
                    batch_stocks = stock_list.iloc[i:i+batch_size]
                    
                    # 创建批次任务
                    future_to_stock = {
                        executor.submit(
                            self.process_single_stock, 
                            str(row['code']), 
                            str(row['name'])
                        ): row 
                        for _, row in batch_stocks.iterrows()
                    }

                    # 处理完成的任务
                    for future in as_completed(future_to_stock):
                        processed_count += 1
                        stock = future_to_stock[future]
                        
                        try:
                            result = future.result()
                            if result:
                                results.append(result)
                            
                            # 每处理100个股票显示一次进度
                            if processed_count % 100 == 0:
                                success_rate = (processed_count - error_count) / processed_count * 100
                                logger.info(
                                    f"处理进度: {processed_count}/{total_stocks} "
                                    f"({processed_count/total_stocks*100:.1f}%) - "
                                    f"成功率: {success_rate:.1f}%"
                                )
                                
                        except Exception as e:
                            error_count += 1
                            logger.error(f"处理股票 {stock['code']} {stock['name']} 失败: {str(e)}")

            # 最终处理结果统计
            if not results:
                raise ValueError("未能获取任何有效数据")
            
            success_rate = (processed_count - error_count) / processed_count * 100
            logger.info(
                f"处理完成 - 总数: {total_stocks}, 成功: {len(results)}, "
                f"失败: {error_count}, 成功率: {success_rate:.1f}%"
            )

            # 转换为DataFrame并优化内存使用
            df = pd.DataFrame(results)
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = df[col].astype('float32')
            
            return df
            
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
        try:
            # 获取历史数据
            history_data = StockAHistoryData()
            max_price_df = history_data.get_history_max_price()
            
            if max_price_df.empty:
                raise ValueError("未能获取历史价格数据")

            # 获取实时数据
            realtime_data = StockARealTimeData()
            realtime_df = realtime_data.get_realtime_data()
            
            if realtime_df.empty:
                raise ValueError("未能获取实时数据")

            # 合并数据前确保列名一致
            realtime_df = realtime_df.rename(columns={'代码': '股票代码'})
            
            # 合并数据
            result_df = pd.merge(
                max_price_df,
                realtime_df,
                on='股票代码',
                how='inner'
            )

            # 转换数据类型并处理异常值
            numeric_columns = ['历史最高', '最高', '流通市值']
            result_df = StockDataProcessor().safe_convert_numeric(result_df, numeric_columns)
            
            # 移除异常值
            result_df = result_df[result_df['历史最高'] > 0]
            result_df = result_df[result_df['最高'] > 0]
            result_df = result_df[result_df['流通市值'] > 0]

            # 筛选数据
            filtered_df = result_df[
                (result_df['历史最高'] <= result_df['最高']) &
                (result_df['流通市值'] > 1e10)
            ].copy()

            if filtered_df.empty:
                logger.warning("筛选后没有符合条件的数据")
                return pd.DataFrame()

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