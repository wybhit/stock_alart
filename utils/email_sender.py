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
                    {df.to_html(index=False)}
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