from pathlib import Path
from typing import Dict, List, Optional

import akshare as ak
from config.config_manager import ConfigTools
from config.constants import MARKET_CODES, MARKET_HOURS
from data.tools import file_exist_or_get_data, file_exist_or_get_data_decorator, logger

import pandas as pd
from pandas_market_calendars import get_calendar


from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

class MarketTimeTools:
    """市场时间工具类"""
    def __init__(self, market: str = "A"):
        self.market = MARKET_CODES[market]
    
    def is_market_time(self, now: Optional[datetime] = None) -> int:
        """判断是否为交易时间
        返回0为未开盘
        返回1为交易中
        返回-1为已收盘
        """
        if self.market not in MARKET_HOURS.keys():
            raise ValueError(f"不支持的市场类型: {self.market}")
        else:
            time_range = MARKET_HOURS[self.market]


        now_time = datetime.now().time()
        
        for (period_start, period_end) in time_range:
            if period_start <= now_time <= period_end:
                return 1
            
        if now_time < time_range[0][0]:
            return 0
        elif now_time > time_range[-1][1]: 
            return -1
        else:
            return 



class TradeDateTools:
    """数据工具类"""
    def __init__(self, market: str = "A"):
        if market not in MARKET_CODES:
            raise ValueError(f"不支持的市场类型: {market}")

        self.market = MARKET_CODES[market]
        self.config = ConfigTools()
        self.last_trade_date = self.get_last_trade_date()
   
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


class DFConvert:
    """股票数据处理基类"""
    def __init__(self):
        pass
    
    @staticmethod
    def safe_convert_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """安全地转换数值类型"""
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df


class StockAHistoryData():
    """A股历史数据处理类"""
    def __init__(self, market: str = "A",hist_data_year: int = 2, n_days_new_high: int = 250):
        """
        Args:
            year (int): 获取历史数据的年数
        """
        self.data_tools = TradeDateTools(market)
        self.last_trade_date = self.data_tools.last_trade_date
        self.hist_data_year = hist_data_year    
        self.hist_data_days = self.hist_data_year*365  
        self.n_days_new_high = n_days_new_high
        self.stock_list = self.get_stock_list()

    @file_exist_or_get_data_decorator(True, "A")
    def get_stock_daily_history(self, code: str) -> pd.DataFrame:
        """获取单个股票的历史日线数据   
        Args:
            code (str): 股票代码
            
        Returns:
            pd.DataFrame: 包含股票历史数据的DataFrame，
            包括日期、开盘价、收盘价、最高价、最低价、成交量、成交额、振幅、涨跌幅、涨跌额、换手率等信息
            
        Raises:
            ValueError: 当未能获取到股票数据时
            Exception: 其他获取数据过程中的异常
        """
        try:
            start_date = (datetime.now() - pd.Timedelta(days=self.hist_data_days)).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=self.last_trade_date,
                adjust="qfq"
            )
            # print(df)
            if df.empty:
                raise ValueError(f"未获取到股票{code}的数据")
            return df
        except Exception as e:
            logger.error(f"获取股票{code}历史数据失败: {str(e)}")
            raise
    
    @file_exist_or_get_data_decorator(True, "A")
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        return ak.stock_info_a_code_name()
    
    def stock_code_name_trans(self, code: str) -> str:
        """股票代码转换为股票名称
        
        Args:
            code (str): 股票代码
            
        Returns:
            tuple: 包含股票代码和股票名称的元组
        """
        if code in self.stock_list['code'].values:
            return code, self.stock_list[self.stock_list['code'] == code]['name'].values[0]
        elif code in self.stock_list['name'].values:
            return self.stock_list[self.stock_list['name'] == code]['code'].values[0],code
        else:
            return None

    def process_single_stock(self, code: str ) -> Optional[Dict]:

        #TODO: 需要优化250日，365天的关系
        try:
            code, name = self.stock_code_name_trans(code)
            
            hist_data = self.get_stock_daily_history(code)
            if hist_data is None or hist_data.empty:
                logger.warning(f"股票 {code} {name} 未获取到历史数据")
                return None

            # 确保列名存在包括日期、开盘价、收盘价、最高价、最低价、成交量、成交额、振幅、涨跌幅、涨跌额、换手率等信息
            required_columns = ['日期', '开盘', '收盘', '最高', '最低', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
            if not all(col in hist_data.columns for col in required_columns):
                logger.warning(f"股票 {code} {name} 数据格式不正确")
                return None

            next_n_days = min(self.n_days_new_high, len(hist_data))
            hist_data = hist_data[-next_n_days:].reset_index(drop=True)

            # 使用向量化操作进行数值转换和缺失值处理
            hist_data['最高'] = pd.to_numeric(hist_data['最高'], errors='coerce')
            hist_data.dropna(subset=['最高'], inplace=True)

            if hist_data.empty:
                return None

            # 使用向量化操作查找最大值索引
            max_price_idx = hist_data['最高'].idxmax()
            max_date = hist_data.at[max_price_idx, '日期']
            days_since_max = len(hist_data) - max_price_idx

            return {
                '股票代码': code,
                '股票名称': name,
                '历史最高': float(hist_data.at[max_price_idx, '最高']),
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
                            str(row['code'])
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

class StockNewHighAnalysis():
    """单个股票数据分析类"""
    def __init__(self, df: pd.DataFrame, n_days_new_high: int = 250, next_n_days: int = 10, n_days_next_new_high: int = 10):
        """包括日期、开盘价、收盘价、最高价、最低价、成交量、成交额、振幅、涨跌幅、涨跌额、换手率等信息"""
        self.df = df
        self.n_days_new_high = min(n_days_new_high, len(self.df))
        self.next_n_days = next_n_days
        self.n_days_next_new_high = n_days_next_new_high

    def new_high_next_n_days_df(self):
        """
        返回:
            pd.DataFrame: 包含新高分析结果的DataFrame
        """
        new_high_list = []
        new_high_flag = 0
        next_n_days_high = 0
        for i in range(len(self.df)-self.n_days_new_high):
            # TODO: 检验验证是否正确    
            if float(self.df.loc[i+self.n_days_new_high, '最高']) == float(self.df.loc[i:i+self.n_days_new_high, '最高'].max()) and new_high_flag == 0:                
                n_days_close,n_days_high,n_days_low = self.n_days_high_low_analysis(self.df.loc[i+self.n_days_new_high, '日期'], self.next_n_days)
                if n_days_high !=""and n_days_low !="":
                    self.df.loc[i+self.n_days_new_high, 'n日后涨跌幅'] = n_days_close
                    self.df.loc[i+self.n_days_new_high, 'n日最大涨幅'] = n_days_high
                    self.df.loc[i+self.n_days_new_high, 'n日最大跌幅'] = n_days_low
                    new_high_list.append(self.df.loc[i+self.n_days_new_high])
                    new_high_flag = 1
            if new_high_flag == 1:
                if next_n_days_high < self.n_days_next_new_high:
                   next_n_days_high += 1
                else:
                    new_high_flag = 0
                    next_n_days_high = 0
        #TODO 继续验证
        if len(new_high_list) == 0:
            #返回空的df[['日期','开盘','收盘','最高','最低','n日最大涨幅','n日最大跌幅']]
            return pd.DataFrame(columns=['日期','开盘','收盘','最高','最低','n日最大涨幅','n日最大跌幅'])
        df = pd.DataFrame(new_high_list)
        
        return df[['日期','开盘','收盘','最高','最低','n日后涨跌幅','n日最大涨幅','n日最大跌幅']]

    def new_high_next_n_days_analysis(self):
        """
        返回:
            pd.DataFrame: 包含新高分析结果的DataFrame
        """
        
        df = self.new_high_next_n_days_df()
        if df.empty:
            return None
        else:
            dic ={
                f'{self.n_days_new_high}日新高天数次数':len(df),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨次数':len(df[df['n日后涨跌幅']>0]),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨平均涨幅':round(df['n日后涨跌幅'].mean(),2),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨最大涨幅为正次数':len(df[df['n日最大涨幅']>0]),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨平均最大涨幅':round(df['n日最大涨幅'].mean(),2),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨最大涨幅为负次数':len(df[df['n日最大跌幅']<0]),
                f'{self.n_days_new_high}日新高后{self.next_n_days}天上涨平均最大跌幅':round(df['n日最大跌幅'].mean(),2),
            }     
            return dic

    
    def n_days_high_low_analysis(self, date: str, n: int = 10) -> tuple:
        """分析指定日期后n天内的最高价和最低价
        
        参数:
            date (str): 起始日期
            n (int): 分析的天数范围
        
        返回:
            tuple: 包含n天内开盘价、收盘价、最高价、最低价
        """
        try:
            index = self.df.loc[self.df['日期'] == date].index[0]
        except IndexError:
            return "", ""  # 如果日期不在数据中，返回空字符串
        if index+1 == len(self.df): 
            return "","",""
        else:
            end_index = min(index + n, len(self.df))
            next_n_days_data = self.df.iloc[index:end_index]
            print(next_n_days_data)
            next_n_days_data = DFConvert().safe_convert_numeric(next_n_days_data, ['收盘', '最高', '最低'])
            #next_n_days_data 最后一日收盘价      
            #TODO:数据格式不对
            # n_days_close = float(next_n_days_data.iloc[-1]['收盘'])
            # n_days_high = float(next_n_days_data['最高'].max())
            # n_days_low =float(next_n_days_data['最低'].min())
            # print(n_days_close,n_days_high,n_days_low)
            n_days_close = round((float(next_n_days_data.iloc[-1]['收盘'])/float(next_n_days_data.iloc[0]['最高'])-1)*100,2)
            n_days_high = round((float(next_n_days_data['最高'].max())/float(next_n_days_data.iloc[0]['最高'])-1)*100,2)
            n_days_low = round((float(next_n_days_data['最低'].min())/float(next_n_days_data.iloc[0]['最高'])-1)*100,2)
            return n_days_close,n_days_high,n_days_low


class StockARealTimeData():
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
            result_df = DFConvert().safe_convert_numeric(result_df, numeric_columns)
            
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





