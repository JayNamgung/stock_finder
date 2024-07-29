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
        # 주가 데이터 가져오기
        df = fdr.DataReader(ticker)
        current_price = df.iloc[-1]['Close']

        # 재무제표 데이터 가져오기
        fs = fdr.DataReader(ticker, 'fs')
        fr = fdr.DataReader(ticker, 'fr')
        
        return current_price, fs, fr
    except Exception as e:
        print(f"Error retrieving data for {ticker}: {str(e)}")
        return None, None, None

def calculate_metrics(current_price, fs, fr):
    try:
        # 재무제표에서 필요한 데이터 추출
        bps = fs.loc['BPS', 'Annual'].iloc[-1]
        eps = fs.loc['EPS', 'Annual'].iloc[-1]
        dps = fs.loc['DPS', 'Annual'].iloc[-1]
        roe = fr.loc['ROE', 'Annual'].iloc[-1] / 100
        dividend_yield = fr.loc['Dividend Yield', 'Annual'].iloc[-1] / 100

        # 지표 계산
        per = current_price / eps if eps != 0 else np.nan
        pbr = current_price / bps if bps != 0 else np.nan
        
        r = 0.1  # 요구수익률 (예시)
        fair_value = round((roe/r) * bps, -1) if r != 0 and bps != 0 else np.nan
        parity = current_price / fair_value if fair_value != 0 else np.nan
        expected_return = (fair_value - current_price) / current_price if current_price != 0 else np.nan

        return {
            'Current Price': current_price,
            'BPS': bps,
            'EPS': eps,
            'DPS': dps,
            'ROE': roe,
            'Dividend Yield': dividend_yield,
            'PER': per,
            'PBR': pbr,
            'Fair Value': fair_value,
            'Parity': parity,
            'Expected Return': expected_return
        }
    except Exception as e:
        print(f"Error calculating metrics: {str(e)}")
        return None

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
        
        current_price, fs, fr = get_financial_data(ticker)
        if current_price is not None and fs is not None and fr is not None:
            metrics = calculate_metrics(current_price, fs, fr)
            if metrics is not None:
                metrics['Symbol'] = ticker
                metrics['Name'] = name
                results.append(metrics)
                processed_stocks += 1
                print(f"Successfully analyzed {name} ({ticker})")
            else:
                error_stocks += 1
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