import streamlit as st
import pandas as pd
from utils.stock_monitor import StockMonitor
import time

def main():
    st.title('股票监控系统')
    
    # 初始化 StockMonitor
    if 'monitor' not in st.session_state:
        st.session_state.monitor = StockMonitor(check_interval=15)
    
    # 添加控制按钮
    col1, col2 = st.columns(2)
    with col1:
        if st.button('开始监控'):
            st.session_state.monitoring = True
    with col2:
        if st.button('停止监控'):
            st.session_state.monitoring = False
            st.session_state.monitor.stop()

    # 显示监控状态
    status_container = st.empty()
    data_container = st.empty()
    
    # 主监控循环
    while 'monitoring' in st.session_state and st.session_state.monitoring:
        monitor = st.session_state.monitor
        
        # 更新状态显示
        if monitor.is_market_time():
            status_container.success('当前为交易时间，正在监控中...')
        else:
            status_container.warning('当前不在交易时间')
        
        # 获取并显示最新数据
        try:
            result_df = monitor.analyzer.process_and_analyze()
            if not result_df.empty:
                with data_container.container():
                    st.subheader('符合条件的股票')
                    st.dataframe(result_df)
                    
                    # 显示新增股票
                    new_stocks = set(result_df['股票代码']) - monitor.previous_stocks
                    if new_stocks:
                        st.subheader('新增股票')
                        new_stocks_df = result_df[result_df['股票代码'].isin(new_stocks)]
                        st.dataframe(new_stocks_df)
            else:
                data_container.info('暂无符合条件的股票')
        except Exception as e:
            data_container.error(f'数据获取失败: {str(e)}')
        
        time.sleep(monitor.check_interval)
        
    # 如果未开始监控或已停止，显示初始状态
    if 'monitoring' not in st.session_state or not st.session_state.monitoring:
        status_container.info('监控未启动')
        data_container.empty()

if __name__ == '__main__':
    main()
