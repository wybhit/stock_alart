from pathlib import Path
import pandas as pd
import time
from datetime import datetime, time as dt_time
from typing import Set

from data.stock_data import StockDataAnalyzer
from utils.email_sender import StockReportSender
from config.config_manager import ConfigTools
from data.tools import logger

# 添加常量配置在文件开头
OUTPUT_DIR = Path("output")
OUTPUT_FILES = {
    'previous_stocks': OUTPUT_DIR / "previous_stocks.csv",
    'new_stocks': OUTPUT_DIR / "new_stocks.csv",
    'result_df': OUTPUT_DIR / "result_df.csv"
}

MARKET_HOURS = {
    'morning': (dt_time(9, 30), dt_time(11, 30)),
    'afternoon': (dt_time(13, 0), dt_time(15, 0))
}

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
        self.previous_file = OUTPUT_FILES['previous_stocks']
        self.is_running = False
        self.email_notifier = EmailNotifier(self.config)

    def _ensure_output_dir(self) -> None:
        """确保输出目录存在"""
        OUTPUT_DIR.mkdir(exist_ok=True)

    def load_previous_stocks(self) -> None:
        """加载之前的股票记录"""
        if not self.previous_file.exists():
            self.previous_stocks = set()
            return

        try:
            df = pd.read_csv(self.previous_file)
            self.previous_stocks = set(df['股票代码'].tolist())
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

    def check_stocks(self) -> None:
        """检查股票状态"""
        try:
            result_df = self.analyzer.process_and_analyze()
            if result_df.empty:
                logger.info("没有符合条件的股票")
                return

            current_stocks = set(result_df['股票代码'].tolist())
            new_stocks = current_stocks - self.previous_stocks
            
            if not new_stocks:
                logger.info("没有新增股票")
                return

            new_stocks_df = result_df[result_df['股票代码'].isin(new_stocks)].copy()
            logger.info(f"发现 {len(new_stocks)} 只新增股票")
            
            self.analyzer.save_results(new_stocks_df, "new_stocks.csv")
            self.email_notifier.send_alerts(new_stocks_df)
            self.previous_stocks = current_stocks
            self.save_current_stocks(current_stocks)
                
        except Exception as e:
            logger.error(f"检查股票状态失败: {str(e)}")

    def is_market_time(self) -> bool:
        """判断是否在交易时间内"""
        now = datetime.now().time()
        
        for period_start, period_end in MARKET_HOURS.values():
            if period_start <= now <= period_end:
                return True
        return False

    def clean_output_files(self) -> None:
        """清理输出文件"""
        try:
            if not OUTPUT_DIR.exists():
                self._ensure_output_dir()
                return

            for file_path in OUTPUT_FILES.values():
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"已删除文件: {file_path.name}")
            
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
        
        # 测试  
        result_df = self.analyzer.process_and_analyze()
        print(result_df)

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

class EmailNotifier:
    """邮件通知类"""
    def __init__(self, config: ConfigTools):
        self.config = config

    def _generate_email_subject(self, df: pd.DataFrame) -> str:
        """生成邮件标题"""
        try:
            # 获取股票名称列表
            stock_names = df['股票名称'].tolist()
            # 如果股票数量大于3个，只显示前3个并加省略号
            if len(stock_names) > 3:
                title_stocks = f"{', '.join(stock_names[:3])}等{len(stock_names)}只股票"
            else:
                title_stocks = f"{', '.join(stock_names)}"
            
            return f"股票提醒: {title_stocks} 符合条件"
        except Exception as e:
            logger.error(f"生成邮件标题失败: {str(e)}")
            return "股票监控提醒"

    def send_alerts(self, df: pd.DataFrame) -> None:
        """发送邮件提醒"""
        try:
            email_sections = [
                section for section in self.config.get_sections() 
                if section.startswith('Email.')
            ]
            
            # 生成邮件标题
            email_subject = self._generate_email_subject(df)
            
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
                        receiver=self.config.get_config(section, "receiver"),
                        subject=email_subject  # 添加自定义标题
                    )
                    logger.info(f"使用 {section} 发送邮件成功")
                except Exception as e:
                    logger.error(f"使用 {section} 发送邮件失败: {str(e)}")
        except Exception as e:
            logger.error(f"发送邮件提醒失败: {str(e)}")