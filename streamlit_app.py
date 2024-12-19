import streamlit as st
import pandas as pd
from utils.stock_monitor import StockMonitor
import time

def initialize_monitor():
    """初始化监控器"""
    if 'monitor' not in st.session_state:
        st.session_state.monitor = StockMonitor(check_interval=15)
        st.session_state.monitor.clean_output_files()
        st.session_state.monitor.load_previous_stocks()

def render_control_buttons():
    """渲染控制按钮"""
    col1, col2 = st.columns(2)
    with col1:
        if st.button('开始监控'):
            st.session_state.monitoring = True
    with col2:
        if st.button('停止监控'):
            st.session_state.monitoring = False

def update_display(monitor, status_container, data_container):
    """更新显示内容"""
    if monitor.is_market_time():
        status_container.success('当前为交易时间，正在监控中...')
        result_df, new_stocks = monitor.check_stocks()
    else:
        status_container.warning('当前不在交易时间')
        result_df, new_stocks = monitor.get_latest_data(), set()

    # 更新数据显示
    with data_container.container():
        if not result_df.empty:
            if new_stocks:
                st.subheader('新增股票')
                new_stocks_df = result_df[result_df['股票代码'].isin(new_stocks)]
                st.dataframe(new_stocks_df)
            
            st.subheader('符合条件的股票')
            st.dataframe(result_df)
        else:
            st.info('暂无符合条件的股票')

def main():
    st.title('股票监控系统')
    
    # 初始化
    initialize_monitor()
    render_control_buttons()

    # 创建显示容器
    status_container = st.empty()
    data_container = st.empty()
    
    # 主监控循环
    if st.session_state.get('monitoring', False):
        try:
            update_display(st.session_state.monitor, status_container, data_container)
        except Exception as e:
            status_container.error(f'监控出错: {str(e)}')
        finally:
            time.sleep(st.session_state.monitor.check_interval)
            st.rerun()
    else:
        status_container.info('监控未启动')
        data_container.empty()

if __name__ == '__main__':
    main()
