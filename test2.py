from data.stock_data import StockAHistoryData, StockNewHighAnalysis

import streamlit as st
from data.stock_data import DFConvert
import plotly.graph_objects as go


def plot_line_with_vlines(df, x_column, y_column, vline_x_positions, title=""):
    """
    创建带有垂直线的折线图
    
    Args:
        df: DataFrame 包含要绘制的数据
        x_column: x轴列名
        y_column: y轴列名
        vline_x_positions: 要添加垂直线的x轴位置列表
        title: 图表标题
    """
    # 创建基础折线图
    fig = go.Figure()
    
    # 添加主要折线
    fig.add_trace(
        go.Scatter(
            x=df[x_column],
            y=df[y_column],
            mode='lines',
            name='价格'
        )
    )
    
    # 添加垂直线
    for x_pos in vline_x_positions:
        fig.add_vline(
            x=x_pos,
            line_width=1,
            line_dash="dash",
            line_color="red",
            opacity=0.5
        )
    
    # 更新布局
    fig.update_layout(
        title=title,
        xaxis_title=x_column,
        yaxis_title=y_column,
        showlegend=True
    )
    
    # 显示图表
    st.plotly_chart(fig)


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


df1 = DFConvert.safe_convert_numeric(one_stock_analysis.df, ["最高"])
df2 =one_stock_analysis.new_high_next_n_days_df()
st.dataframe(df2)

#取df1的最后250行
df3 = df1.tail(250)

plot_line_with_vlines(df3,"日期","最高",df2.loc[:,"日期"])



# 在my_table中添加竖线



# one_stock_analysis = StockNewHighAnalysis(stock_data.get_stock_daily_history(stock_code), n_days_new_high,next_n_days,n_days_next_new_high)

# one_stock_analysis.new_high_next_n_days_analysis()