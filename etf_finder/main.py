import yfinance as yf
import json
import os
import time
from datetime import datetime
import requests
import pandas as pd
import io

def get_us_etf_list():
    url = "https://www.nasdaq.com/api/v1/screener?page=1&pageSize=10000&filterkey=asset-class&filtervalue=etf"
    response = requests.get(url)
    data = response.json()
    etfs = [etf['symbol'] for etf in data['data']['table']['rows']]
    return etfs

def get_korean_etf_list():
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_payload = {
        "locale": "ko_KR",
        "mktId": "ETF",
        "trdDd": datetime.now().strftime("%Y%m%d"),
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT04501"
    }
    otp_response = requests.post(url, data=otp_payload)
    otp = otp_response.text

    download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    download_response = requests.post(download_url, data={"code": otp})
    
    df = pd.read_csv(io.StringIO(download_response.content.decode('euc-kr')))
    etfs = df['종목코드'].tolist()
    return [f"{code}.KS" for code in etfs]  # Yahoo Finance 형식으로 변환

def get_etf_data(symbol, period="5y"):
    try:
        etf = yf.Ticker(symbol)
        
        # ETF 정보
        info = etf.info
        
        # 주가 데이터
        history = etf.history(period=period)
        
        # 상위 10개 보유 종목
        holdings = etf.holdings
        
        return {
            "info": info,
            "history": history.reset_index().to_dict(orient="records"),
            "holdings": holdings
        }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
        return None

def save_as_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_as_text(data, filename, chunk_size=1000):
    with open(filename, 'w', encoding='utf-8') as f:
        text = json.dumps(data, ensure_ascii=False)
        chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
        for chunk in chunks:
            f.write(chunk + '\n\n')

def main():
    print("Fetching US ETF list...")
    us_etfs = get_us_etf_list()
    print(f"Found {len(us_etfs)} US ETFs")
    
    print("Fetching Korean ETF list...")
    korean_etfs = get_korean_etf_list()
    print(f"Found {len(korean_etfs)} Korean ETFs")
    
    all_etfs = us_etfs + korean_etfs
    
    for symbol in all_etfs:
        print(f"Fetching data for {symbol}...")
        data = get_etf_data(symbol)
        
        if data:
            # JSON 파일로 저장
            json_filename = f"data/{symbol}_data.json"
            os.makedirs(os.path.dirname(json_filename), exist_ok=True)
            save_as_json(data, json_filename)
            print(f"Saved JSON file: {json_filename}")
            
            # 텍스트 파일로 저장
            text_filename = f"data/{symbol}_data.txt"
            save_as_text(data, text_filename)
            print(f"Saved text file: {text_filename}")
        
        print(f"Completed processing {symbol}\n")
        
        # API 사용량 제한을 위한 대기
        time.sleep(1)

if __name__ == "__main__":
    main()