import yfinance as yf
from yahooquery import Ticker
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_us_etf_list(limit=5):
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
                etfs.append((symbol, 'US'))
    
    return etfs

def get_kr_etf_list(limit=5):
    base_url = "https://finance.yahoo.com/lookup"
    etfs = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    kr_etf_providers = ['KODEX', 'TIGER', 'ARIRANG', 'RISE', 'HANARO']
    
    for provider in kr_etf_providers:
        params = {
            's': provider,
            't': 'E',      # ETFs
            'm': 'KR',     # Market: Korea
            'f': '0',      # All ETFs
        }

        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'W(100%)'})
        
        if table:
            rows = table.find_all('tr')[1:]  # Skip header row
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 1:
                    symbol = cols[0].text.strip()
                    etfs.append((symbol, 'KR'))
                    
                    if len(etfs) >= limit:
                        return etfs
        
        time.sleep(1)  # 요청 사이에 잠시 대기
    
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
                holding_info = {
                    'name': row.get('holdingName', 'N/A'),
                    'symbol': row.get('symbol', 'N/A'),
                    'percent': f"{row.get('holdingPercent', 'N/A')}%"
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

def get_etf_data(symbol, country):
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
        if country == 'US':
            top_holdings = get_top_holdings(symbol)
        else:
            print(f"Holdings data not available for {country} ETFs")
            top_holdings = []
        
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
    print("Fetching 5 US ETFs...")
    us_etfs = get_us_etf_list(5)
    print(f"Found {len(us_etfs)} US ETFs")
    
    print("Fetching 5 Korean ETFs...")
    kr_etfs = get_kr_etf_list(5)
    print(f"Found {len(kr_etfs)} Korean ETFs")
    
    all_etfs = us_etfs + kr_etfs
    
    all_etf_data = []
    
    for symbol, country in all_etfs:
        print(f"Fetching data for {symbol}...")
        data = get_etf_data(symbol, country)
        
        if data:
            all_etf_data.append(data)
        
        print(f"Completed processing {symbol}\n")
        
        # API 사용량 제한을 위한 대기
        time.sleep(2)
    
    # ETF 정보와 top 5 보유 종목을 텍스트 파일로 저장
    text_filename = "data/etf_data_test.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_etf_data, text_filename)
    print(f"Saved ETF data to text file: {text_filename}")

if __name__ == "__main__":
    main()