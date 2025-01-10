from data.stock_data import StockAHistoryData, StockNewHighAnalysis

import streamlit as st
from data.stock_data import DFConvert

stock_data = StockAHistoryData()

# print(stock_data.process_single_stock("601137", 250))

#要求输入股票代码
stock_code = st.text_input("请输入股票代码",value="601137")
#要求输入新高天数
n_days_new_high = st.number_input("请输入新高天数", min_value=1, value=250)
#要求输入n日后分析天数
next_n_days = st.number_input("请输入n日后分析天数", min_value=1, value=10)
#要求输入忽略n日后新高的天数
n_days_next_new_high = st.number_input("请输入忽略n日后新高的天数", min_value=1, value=10)

one_stock_analysis = StockNewHighAnalysis(stock_data.get_stock_daily_history(stock_code), n_days_new_high=n_days_new_high,next_n_days=next_n_days,n_days_next_new_high=n_days_next_new_high)

one_stock_analysis.new_high_next_n_days_analysis()

st.write(one_stock_analysis.new_high_next_n_days_analysis())
st.dataframe(one_stock_analysis.new_high_next_n_days_df())

st.line_chart(DFConvert.safe_convert_numeric(one_stock_analysis.df, ["最高"]),x="日期",y="最高")


# one_stock_analysis = StockNewHighAnalysis(stock_data.get_stock_daily_history(stock_code), n_days_new_high,next_n_days,n_days_next_new_high)

# one_stock_analysis.new_high_next_n_days_analysis()