from pathlib import Path
import pandas as pd
import time
from datetime import datetime, time as dt_time
from typing import Set

from data.stock_data import StockDataAnalyzer
from utils.email_sender import StockReportSender
from config.config_manager import ConfigTools
from data.tools import logger

class StockMonitor:
    """股票监控类"""
    def __init__(self, check_interval: int = 60):
        """
        初始化监控器
        
        Args:
            check_interval: 检查间隔（秒）
        """
        self.check_interval = check_interval
        self.analyzer = StockDataAnalyzer()
        self.config = ConfigTools()
        self.previous_stocks: Set[str] = set()
        self.previous_file = Path("output/previous_stocks.csv")
        self.is_running = False

    def load_previous_stocks(self) -> None:
        """加载之前的股票记录"""
        try:
            if self.previous_file.exists():
                df = pd.read_csv(self.previous_file)
                self.previous_stocks = set(df['股票代码'].tolist())
            else:
                self.previous_stocks = set()
        except Exception as e:
            logger.error(f"加载之前的股票记录失败: {str(e)}")
            self.previous_stocks = set()

    def save_current_stocks(self, stock_codes: Set[str]) -> None:
        """保存当前的股票记录"""
        try:
            df = pd.DataFrame({'股票代码': list(stock_codes)})
            df.to_csv(self.previous_file, index=False)
        except Exception as e:
            logger.error(f"保存当前股票记录失败: {str(e)}")

    def send_email_alerts(self, df: pd.DataFrame) -> None:
        """发送邮件提醒"""
        try:
            email_sections = [
                section for section in self.config.get_sections() 
                if section.startswith('Email.')
            ]
            
            for section in email_sections:
                try:
                    report_sender = StockReportSender(
                        smtp_server=self.config.get_config(section, "smtp_server"),
                        smtp_port=int(self.config.get_config(section, "smtp_port")),
                        sender=self.config.get_config(section, "sender"),
                        password=self.config.get_config(section, "password")
                    )
                    report_sender.send_stock_report(
                        df=df,
                        receiver=self.config.get_config(section, "receiver")
                    )
                    logger.info(f"使用 {section} 发送邮件成功")
                except Exception as e:
                    logger.error(f"使用 {section} 发送邮件失败: {str(e)}")
        except Exception as e:
            logger.error(f"发送邮件提醒失败: {str(e)}")

    def check_stocks(self) -> None:
        """检查股票状态"""
        try:
            result_df = self.analyzer.process_and_analyze()
            
            if not result_df.empty:
                # 获取当前符合条件的股票代码集合
                current_stocks = set(result_df['股票代码'].tolist())
                
                # 找出新增的股票
                new_stocks = current_stocks - self.previous_stocks
                
                if new_stocks:
                    # 筛选出新增股票的完整数据
                    new_stocks_df = result_df[result_df['股票代码'].isin(new_stocks)].copy()
                    logger.info(f"发现 {len(new_stocks)} 只新增股票")
                    
                    # 保存新增股票结果
                    self.analyzer.save_results(new_stocks_df, "new_stocks.csv")
                    
                    # 发送邮件
                    self.send_email_alerts(new_stocks_df)
                else:
                    logger.info("没有新增股票")
                
                # 更新股票记录
                self.previous_stocks = current_stocks
                self.save_current_stocks(current_stocks)
            else:
                logger.info("没有符合条件的股票")
                
        except Exception as e:
            logger.error(f"检查股票状态失败: {str(e)}")

    def is_market_time(self) -> bool:
        """判断是否在交易时间内"""
        now = datetime.now().time()
        morning_start = dt_time(9, 30)
        morning_end = dt_time(11, 30)
        afternoon_start = dt_time(13, 0)
        afternoon_end = dt_time(15, 0)
        
        # return (morning_start <= now <= morning_end) or (afternoon_start <= now <= afternoon_end)
        return True
    def clean_output_files(self) -> None:
        """清理输出文件"""
        try:
            output_dir = Path("output")
            if output_dir.exists():
                # 清理特定的文件
                files_to_clean = [
                    "previous_stocks.csv",
                    "new_stocks.csv",
                    "result_df.csv"
                ]
                for file in files_to_clean:
                    file_path = output_dir / file
                    if file_path.exists():
                        file_path.unlink()
                        logger.info(f"已删除文件: {file}")
            
            # 确保输出目录存在
            output_dir.mkdir(exist_ok=True)
            logger.info("输出目录已清理")
            
        except Exception as e:
            logger.error(f"清理输出文件失败: {str(e)}")

    def start(self) -> None:
        """启动监控"""
        logger.info("启动股票监控程序")
        self.is_running = True
        
        # 清理输出文件
        self.clean_output_files()
        
        # 初始化
        self.load_previous_stocks()
        
        while self.is_running:
            try:
                if self.is_market_time():
                    self.check_stocks()
                else:
                    logger.info("当前不在交易时间")
                
                logger.info("等待下一次检查...")
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                self.stop()
            except Exception as e:
                logger.error(f"监控异常: {str(e)}")
                time.sleep(self.check_interval)

    def stop(self) -> None:
        """停止监控"""
        self.is_running = False
        logger.info("监控程序已停止")