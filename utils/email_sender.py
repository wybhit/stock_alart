import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Union
import pandas as pd
from data.tools import logger

class StockReportSender:
    def __init__(self, smtp_server: str, smtp_port: int, sender: str, password: str):
        self.email_sender = EmailSender(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            sender=sender,
            password=password
        )
        
    def send_stock_report(self, df: pd.DataFrame, receiver: str, subject: str = None) -> None:
        """发送股票报告
        
        Args:
            df: 股票数据
            receiver: 接收者邮箱
            subject: 自定义邮件标题
        """
        if subject is None:
            subject = "股票监控提醒"
        
        try:
            if df.empty:
                logger.warning("没有数据需要发送")
                return

            # 处理收件人列表
            if isinstance(receiver, str):
                receivers = [r.strip() for r in receiver.split(',')]
            else:
                receivers = receiver

            # 选择需要展示的列
            display_columns = [
                '股票代码', '股票名称', '最高', '涨跌幅', 
                '历史最高', '历史最高日期', '距今交易日数', '流通市值'
            ]
            display_df = df[display_columns].copy()
            
            # 格式化数值
            if '流通市值' in display_df.columns:
                display_df['流通市值'] = (display_df['流通市值'] / 1e8).round(2)
                display_df = display_df.rename(columns={
                    '流通市值': '流通市值(亿)',
                    '最高': '当前价格',
                    '涨跌幅': '涨跌幅(%)'
                })
            
            # 发送给每个收件人
            for receiver_email in receivers:
                if self.email_sender.send_stock_report(receiver_email.strip(), display_df, subject):
                    logger.info(f"邮件发送成功: {receiver_email}")
            
        except ValueError as ve:
            logger.error(f"数据处理错误: {str(ve)}")
        except smtplib.SMTPException as smtp_e:
            logger.error(f"SMTP错误: {str(smtp_e)}")
        except Exception as e:
            logger.error(f"发送邮件报告失败: {str(e)}")

class EmailSender:
    def __init__(self, smtp_server: str, smtp_port: int, sender: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender = sender
        self.password = password

    def send_stock_report(self, receiver: str, df: pd.DataFrame, subject: Optional[str] = None) -> bool:
        """发送股票报告邮件
        
        Returns:
            bool: 发送是否成功
        """
        try:
            if df.empty:
                return False
            
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = receiver
            msg['Subject'] = subject or '股票新高提醒'

            # 生成HTML内容
            html_content = f"""
            <html>
                <head>
                    <style>
                        table {{
                            border-collapse: collapse;
                            margin: 10px 0;
                            font-size: 14px;
                            width: 100%;
                        }}
                        th, td {{
                            border: 1px solid #ddd;
                            padding: 8px;
                            text-align: left;
                        }}
                        th {{
                            background-color: #f2f2f2;
                        }}
                        .positive {{
                            color: red;
                        }}
                        .negative {{
                            color: green;
                        }}
                    </style>
                </head>
                <body>
                    <h3>今日创新高的股票：</h3>
                    {self._generate_html_table(df)}
                    <p style="color: gray; font-size: 12px;">
                        注：<br>
                        1. 流通市值单位为亿元<br>
                        2. 距今交易日数为距离上次历史新高的交易日数量
                    </p>
                </body>
            </html>
            """
            
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.sender, self.password)
                server.send_message(msg)
                return True
                
        except Exception as e:
            logger.error(f"发送邮件失败: {str(e)}")
            return False

    def _generate_html_table(self, df: pd.DataFrame) -> str:
        """生成带有超链接的HTML表格"""
        df = df.copy()
        df['股票名称'] = df.apply(lambda row: f'<a href="https://xueqiu.com/S/{self._get_stock_prefix(row["股票代码"])}{row["股票代码"]}">{row["股票名称"]}</a>', axis=1)
        df['股票代码'] = df.apply(lambda row: f'<a href="https://quote.eastmoney.com/{row["股票代码"]}.html" target="_blank">{row["股票代码"]}</a>', axis=1)
        return df.to_html(index=False, escape=False)

    def _get_stock_prefix(self, stock_code: str) -> str:
        """根据股票代码返回前缀"""
        # 假设股票代码以6开头的是上证（SH），其他的是深证（SZ）
        if stock_code.startswith('6'):
            return 'SH'
        else:
            return 'SZ'