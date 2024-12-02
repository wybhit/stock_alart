from data.stock_data import StockDataAnalyzer

def main():
    analyzer = StockDataAnalyzer()
    result_df = analyzer.process_and_analyze()
    print(result_df)
    analyzer.save_results(result_df)

if __name__ == "__main__":
    main()