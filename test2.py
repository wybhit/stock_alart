from data.stock_data import StockAHistoryData, OneStockAnalysis

if __name__ == "__main__":
    stock_data = StockAHistoryData()

    # print(stock_data.process_single_stock("601137", 250))

    one_stock_analysis = OneStockAnalysis(stock_data.get_stock_daily_history("601137"), 800
    )
    print(one_stock_analysis.new_high_analysis(10))
