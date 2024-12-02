from utils.stock_monitor import StockMonitor

def main():
    monitor = StockMonitor(check_interval=60)  # 每60秒检查一次
    monitor.start()

if __name__ == "__main__":
    main()