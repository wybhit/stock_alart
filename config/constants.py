# 市场代码映射
from datetime import time as dt_time


MARKET_CODES = {
    "A": "XSHG",#上海交易所
    "US": "NYSE",
    "HK": "HKG"
}

MARKET_HOURS = {
    "XSHG": [(dt_time(9, 15), dt_time(11, 30)), (dt_time(13, 0), dt_time(15, 0))]
}

A_MARKET_HOURS = {
    'morning': (dt_time(9, 15), dt_time(11, 30)),
    'afternoon': (dt_time(13, 0), dt_time(15, 0))
}