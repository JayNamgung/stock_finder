import yfinance as yf
from yahooquery import Ticker
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

# 사용자가 원하는 ETF 개수를 지정할 수 있는 전역 변수
ETF_COUNT = 10

def get_us_etf_list(limit):
    base_url = "https://finance.yahoo.com/etfs"
    etfs = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    url = f"{base_url}?count=100&offset=0"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'W(100%)'})
    
    if table:
        rows = table.find_all('tr')[1:]  # Skip header row
        for row in rows[:limit]:  # Only process up to the limit
            cols = row.find_all('td')
            if len(cols) >= 6:  # Ensure we have enough columns
                symbol = cols[0].text.strip()
                etfs.append(symbol)
    
    return etfs

def get_top_holdings(symbol):
    try:
        print(f"Fetching holdings for {symbol} using yahooquery")
        etf = Ticker(symbol)
        
        # Get fund top holdings
        holdings_data = etf.fund_top_holdings
        print(f"Raw holdings data: {holdings_data}")
        
        if isinstance(holdings_data, pd.DataFrame) and not holdings_data.empty:
            holdings = []
            for _, row in holdings_data.iterrows():
                holding_percent = row.get('holdingPercent', 0) * 100  # 100을 곱하여 실제 퍼센트 값으로 변환
                holding_info = {
                    'name': row.get('holdingName', 'N/A'),
                    'symbol': row.get('symbol', 'N/A'),
                    'percent': f"{holding_percent:.2f}%"  # 소수점 둘째 자리까지 포맷팅
                }
                holdings.append(holding_info)
                print(f"Added holding: {holding_info}")
            
            return holdings[:5]  # Return only top 5 holdings
        else:
            print(f"No holdings data available for {symbol}")
            return []
    except Exception as e:
        print(f"Error fetching top holdings for {symbol}: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return []

def get_etf_data(symbol):
    try:
        etf = yf.Ticker(symbol)
        
        # 필요한 ETF 정보만 추출
        info = etf.info
        required_info = {
            "symbol": info.get("symbol", symbol),
            "shortName": info.get("shortName", "N/A"),
            "longName": info.get("longName", "N/A"),
            "category": info.get("category", "N/A"),
            "longBusinessSummary": info.get("longBusinessSummary", "No description available.")
        }
        
        # Top 5 보유 종목
        top_holdings = get_top_holdings(symbol)
        
        return {
            "info": required_info,
            "top_holdings": top_holdings
        }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
        return None

def save_all_text(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for etf in data:
            info = etf['info']
            f.write(f"Symbol: {info['symbol']}\n")
            f.write(f"Short Name: {info['shortName']}\n")
            f.write(f"Long Name: {info['longName']}\n")
            f.write(f"Category: {info['category']}\n")
            f.write(f"\nDescription:\n{info['longBusinessSummary']}\n\n")
            f.write("Top 5 Holdings:\n")
            for holding in etf['top_holdings']:
                f.write(f"- {holding['name']} ({holding['symbol']}): {holding['percent']}\n")
            f.write('\n' + '='*50 + '\n\n')

def main():
    print(f"Fetching {ETF_COUNT} US ETFs...")
    us_etfs = get_us_etf_list(ETF_COUNT)
    print(f"Found {len(us_etfs)} US ETFs")
    
    all_etf_data = []
    
    for symbol in us_etfs:
        print(f"Fetching data for {symbol}...")
        data = get_etf_data(symbol)
        
        if data:
            all_etf_data.append(data)
        
        print(f"Completed processing {symbol}\n")
        
        # API 사용량 제한을 위한 대기
        time.sleep(2)
    
    # ETF 정보와 top 5 보유 종목을 텍스트 파일로 저장
    text_filename = "data/etf_data.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_etf_data, text_filename)
    print(f"Saved ETF data to text file: {text_filename}")

if __name__ == "__main__":
    main()