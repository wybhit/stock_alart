from data.stock_data import TradeDateTools


if __name__ == "__main__":
    trade_date_tools = TradeDateTools()
    print(trade_date_tools.is_market_time())
