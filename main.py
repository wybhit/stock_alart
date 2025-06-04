from utils.stock_monitor import StockMonitor
from data.stock_data import MarketTimeTools
import time

def main():
    monitor = StockMonitor(check_interval=600)  # 每15秒检查一次
    market_time_tools = MarketTimeTools()
    monitor.start()
    while True:
        if market_time_tools.is_market_time() == 1:
            monitor.start()
        else:
            time.sleep(15)

if __name__ == "__main__":
    main()
    