import pandas as pd
import yfinance as yf
from stockstats import wrap
from typing import Annotated
import os
from .config import get_config


class StockstatsUtils:
    @staticmethod
    def get_stock_stats(
        symbol: Annotated[str, "ticker symbol for the company"],
        indicator: Annotated[
            str, "quantitative indicators based off of the stock data for the company"
        ],
        curr_date: Annotated[
            str, "curr date for retrieving stock price data, YYYY-mm-dd"
        ],
        data_dir: Annotated[
            str,
            "directory where the stock data is stored.",
        ],
        online: Annotated[
            bool,
            "whether to use online tools to fetch data or offline tools. If True, will use online tools.",
        ] = False,
    ):
        df = None
        data = None

        if not online:
            try:
                data = pd.read_csv(
                    os.path.join(
                        data_dir,
                        f"{symbol}-YFin-data-2015-01-01-2025-03-25.csv",
                    )
                )
                # 确保 Date 列是字符串格式
                if 'Date' in data.columns:
                    if data['Date'].dtype != 'object':
                        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
                    elif not isinstance(data['Date'].iloc[0], str):
                        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
                
                df = wrap(data)
            except FileNotFoundError:
                raise Exception("Stockstats fail: Yahoo Finance data not fetched yet!")
        else:
            # Get today's date as YYYY-mm-dd to add to cache
            today_date = pd.Timestamp.today()
            curr_date_dt = pd.to_datetime(curr_date)

            end_date = today_date
            start_date = today_date - pd.DateOffset(years=15)
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            # Get config and ensure cache directory exists
            config = get_config()
            os.makedirs(config["data_cache_dir"], exist_ok=True)

            data_file = os.path.join(
                config["data_cache_dir"],
                f"{symbol}-YFin-data-{start_date}-{end_date}.csv",
            )

            if os.path.exists(data_file):
                data = pd.read_csv(data_file)
                # 检查 Date 列的类型
                if 'Date' in data.columns:
                    # 如果是字符串，先转为 datetime
                    if data['Date'].dtype == 'object':
                        data['Date'] = pd.to_datetime(data['Date'])
            else:
                data = yf.download(
                    symbol,
                    start=start_date,
                    end=end_date,
                    multi_level_index=False,
                    progress=False,
                    auto_adjust=True,
                )
                data = data.reset_index()
                data.to_csv(data_file, index=False)

            # 确保 Date 列存在并转换为字符串格式
            if 'Date' in data.columns:
                data['Date'] = pd.to_datetime(data['Date']).dt.strftime("%Y-%m-%d")
            
            df = wrap(data)
            curr_date = curr_date_dt.strftime("%Y-%m-%d")

        df[indicator]  # trigger stockstats to calculate the indicator
        
        # 检查 Date 列是否存在
        if 'Date' not in df.columns:
            # Date 可能在索引中
            if hasattr(df.index, 'strftime'):
                # 索引是 datetime，转换为字符串
                date_series = df.index.strftime('%Y-%m-%d')
                matching_idx = date_series == curr_date
                if matching_idx.any():
                    return df.loc[matching_idx, indicator].iloc[0]
            return "N/A: Not a trading day (weekend or holiday)"
        
        # 使用精确匹配而不是 startswith
        matching_rows = df[df["Date"] == curr_date]

        if not matching_rows.empty:
            indicator_value = matching_rows[indicator].values[0]
            return indicator_value
        else:
            return "N/A: Not a trading day (weekend or holiday)"
