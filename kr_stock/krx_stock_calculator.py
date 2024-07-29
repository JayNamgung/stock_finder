import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime

def get_krx_tickers():
    # KRX 상장 종목 전체 가져오기
    df_krx = fdr.StockListing('KRX')
    # 열 이름 확인 및 필요한 경우 매핑
    if 'Symbol' not in df_krx.columns:
        if 'Code' in df_krx.columns:
            df_krx = df_krx.rename(columns={'Code': 'Symbol'})
        else:
            print("Warning: 'Symbol' or 'Code' column not found in KRX listing.")
            print("Available columns:", df_krx.columns)
    if 'Name' not in df_krx.columns and 'Name(Korean)' in df_krx.columns:
        df_krx = df_krx.rename(columns={'Name(Korean)': 'Name'})
    return df_krx

def get_financial_data(ticker):
    # 재무제표 및 주가 정보 가져오기
    try:
        fs = fdr.FnGuide.financial_statements(ticker)
        fr = fdr.FnGuide.financial_ratio(ticker)
        df = fdr.DataReader(ticker)
        current_price = df.iloc[-1]['Close']
        return fs, fr, current_price
    except Exception as e:
        print(f"Failed to retrieve data for {ticker}: {str(e)}")
        return None, None, None

def calculate_metrics(fs, fr, current_price):
    # 필요한 지표 계산
    try:
        bps = fs.loc['BPS'].iloc[-1]
        eps = fs.loc['EPS'].iloc[-1]
        dps = fs.loc['DPS'].iloc[-1]
        roe = fr.loc['ROE'].iloc[-1] / 100  # 비율로 변환
        dividend_yield = fr.loc['Dividend Yield'].iloc[-1] / 100  # 비율로 변환
        per = current_price / eps
        pbr = current_price / bps
        
        # 적정주가 계산 (r은 요구수익률로, 여기서는 예시로 0.1 사용)
        r = 0.1
        fair_value = round((roe/r) * bps, -1)
        
        # 패리티 계산
        parity = current_price / fair_value
        
        # 기대수익률 계산
        expected_return = (fair_value - current_price) / current_price
        
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
        print(f"Failed to calculate metrics: {str(e)}")
        return None

def main():
    # KRX 상장 종목 가져오기
    krx_tickers = get_krx_tickers()
    
    # 결과를 저장할 리스트
    results = []
    
    # 각 종목에 대해 데이터 가져오기 및 분석
    for index, row in krx_tickers.iterrows():
        if 'Symbol' not in row or 'Name' not in row:
            print(f"Warning: Required columns not found in row. Skipping. Row data: {row}")
            continue
        
        ticker = row['Symbol']
        name = row['Name']
        
        fs, fr, current_price = get_financial_data(ticker)
        if fs is not None and fr is not None and current_price is not None:
            metrics = calculate_metrics(fs, fr, current_price)
            if metrics is not None:
                metrics['Symbol'] = ticker
                metrics['Name'] = name
                results.append(metrics)
                print(f"Successfully analyzed {name} ({ticker})")
        
    # 결과를 DataFrame으로 변환
    df_results = pd.DataFrame(results)
    
    # 결과를 파일로 저장
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"stock_analysis_results_{current_time}.csv"
    df_results.to_csv(filename, index=False)
    print(f"Analysis results saved to {filename}")

if __name__ == "__main__":
    main()