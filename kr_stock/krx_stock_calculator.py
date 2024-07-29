import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os

def get_krx_tickers():
    df_krx = fdr.StockListing('KRX')
    print(f"Total number of stocks: {len(df_krx)}")
    print(f"Columns in KRX listing: {df_krx.columns}")
    return df_krx

def get_financial_data(ticker):
    try:
        # 주가 데이터만 가져오기
        df = fdr.DataReader(ticker)
        current_price = df.iloc[-1]['Close']
        return current_price
    except Exception as e:
        print(f"Error retrieving data for {ticker}: {str(e)}")
        return None

def calculate_metrics(current_price):
    # 간단한 지표만 계산
    return {
        'Current Price': current_price,
    }

def main():
    krx_tickers = get_krx_tickers()
    results = []
    total_stocks = min(10, len(krx_tickers))  # 테스트를 위해 10개로 제한
    processed_stocks = 0
    error_stocks = 0

    for index, row in krx_tickers.iloc[:total_stocks].iterrows():
        ticker = row.get('Symbol') or row.get('Code')
        name = row.get('Name') or row.get('Name(Korean)')
        
        if not ticker or not name:
            print(f"Missing ticker or name for row: {row}")
            error_stocks += 1
            continue

        print(f"Processing {name} ({ticker}) - {processed_stocks + 1}/{total_stocks}")
        
        current_price = get_financial_data(ticker)
        if current_price is not None:
            metrics = calculate_metrics(current_price)
            metrics['Symbol'] = ticker
            metrics['Name'] = name
            results.append(metrics)
            processed_stocks += 1
            print(f"Successfully analyzed {name} ({ticker})")
        else:
            error_stocks += 1
        
        time.sleep(1)  # 서버 부하 감소를 위한 지연

    df_results = pd.DataFrame(results)
    
    # 결과 파일 저장
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"stock_analysis_results_{current_time}.csv"
    save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(save_dir, exist_ok=True)
    full_path = os.path.join(save_dir, filename)
    
    df_results.to_csv(full_path, index=False)
    print(f"Analysis results saved to {full_path}")
    print(f"Total stocks: {total_stocks}, Processed: {processed_stocks}, Errors: {error_stocks}")

if __name__ == "__main__":
    main()