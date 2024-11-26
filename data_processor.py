from get_Stock_Data import Stock_A_History_Date,Stock_A_Real_Time_Data
import pandas as pd

class DataAnalyzer():
    def __init__(self):
        pass

    def new_high_stock(self):
        a = Stock_A_History_Date()
        a_df = a.get_history_max_price()
        b = Stock_A_Real_Time_Data()
        b_df = b.get_stock_zh_a_realtime()
        #合并a_df和b_df,以股票代码为key
        result_df = pd.merge(a_df, b_df, left_on='股票代码', right_on='代码', how='inner')
        result_df.to_csv("result_df.csv",index=False)

        
        result_df['历史最高'] = result_df['历史最高'].astype(float)
        result_df['最高'] = result_df['最高'].astype(float)
        result_df['流通市值'] = result_df['流通市值'].astype(int)
        
        
        #筛选最高价小于最新价，流通市值大于100亿
        result_df = result_df[result_df["历史最高"]<result_df["最高"]]
        result_df = result_df[result_df["流通市值"]>10000000000]
        #result_df重新编号,按流通市值由大到小排序
        result_df = result_df.sort_values(by="流通市值",ascending=False)
        result_df = result_df.reset_index(drop=True)
        result_df.to_csv("result_df.csv",index=False)



        print(result_df)