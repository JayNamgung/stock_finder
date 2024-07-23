import yfinance as yf
from yahooquery import Ticker
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
from requests.exceptions import RequestException
import json
import concurrent.futures

# 사용자가 원하는 ETF 개수를 지정할 수 있는 전역 변수
ETF_COUNT = 4000  # 원하는 ETF 수로 설정
MAX_WORKERS = 10  # 동시에 실행할 최대 worker 수

def get_us_etf_list(limit):
    base_url = "https://finance.yahoo.com/etfs"
    etfs = []
    offset = 0
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"Starting to fetch {limit} US ETFs...")

    while len(etfs) < limit:
        url = f"{base_url}?count=100&offset={offset}"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'class': 'W(100%)'})
            
            if table:
                rows = table.find_all('tr')[1:]  # Skip header row
                for row in rows:
                    if len(etfs) >= limit:
                        break
                    cols = row.find_all('td')
                    if len(cols) >= 6:  # Ensure we have enough columns
                        symbol = cols[0].text.strip()
                        etfs.append(symbol)
                        
                        # 100개 단위로 로그 출력
                        if len(etfs) % 100 == 0:
                            print(f"Fetched {len(etfs)} ETFs so far...")
            
            offset += 100
            time.sleep(random.uniform(1, 3))  # Random delay between requests
        except RequestException as e:
            print(f"Error fetching ETF list: {e}")
            time.sleep(60)  # Wait for 60 seconds before retrying
    
    print(f"Completed fetching {len(etfs)} ETFs.")
    return etfs[:limit]

def get_top_holdings(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"Fetching holdings for {symbol} using yahooquery (Attempt {attempt + 1})")
            etf = Ticker(symbol)
            
            holdings_data = etf.fund_top_holdings
            
            if isinstance(holdings_data, pd.DataFrame) and not holdings_data.empty:
                holdings = []
                for _, row in holdings_data.iterrows():
                    holding_percent = row.get('holdingPercent', 0) * 100
                    holding_info = {
                        'name': row.get('holdingName', 'N/A'),
                        'symbol': row.get('symbol', 'N/A'),
                        'percent': f"{holding_percent:.2f}%"
                    }
                    holdings.append(holding_info)
                
                return holdings[:5]  # Return only top 5 holdings
            else:
                print(f"No holdings data available for {symbol}")
                return []
        except Exception as e:
            print(f"Error fetching top holdings for {symbol}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))  # Random delay before retrying
            else:
                print(f"Max retries reached for {symbol}")
                return []

def get_etf_data(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            etf = yf.Ticker(symbol)
            
            info = etf.info
            required_info = {
                "symbol": info.get("symbol", symbol),
                "shortName": info.get("shortName", "N/A"),
                "longName": info.get("longName", "N/A"),
                "category": info.get("category", "N/A"),
                "longBusinessSummary": info.get("longBusinessSummary", "No description available.")
            }
            
            top_holdings = get_top_holdings(symbol)
            
            return {
                "info": required_info,
                "top_holdings": top_holdings
            }
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))  # Random delay before retrying
            else:
                print(f"Max retries reached for {symbol}")
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

def save_progress(processed_etfs, filename='progress.json'):
    with open(filename, 'w') as f:
        json.dump(processed_etfs, f)

def load_progress(filename='progress.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}

def process_etf(symbol, processed_etfs):
    if symbol in processed_etfs:
        print(f"Skipping already processed ETF: {symbol}")
        return processed_etfs[symbol]

    print(f"Fetching data for {symbol}...")
    data = get_etf_data(symbol)
    
    if data:
        processed_etfs[symbol] = data
        save_progress(processed_etfs)
    
    print(f"Completed processing {symbol}\n")
    return data

def main():
    print(f"Fetching {ETF_COUNT} US ETFs...")
    us_etfs = get_us_etf_list(ETF_COUNT)
    print(f"Found {len(us_etfs)} US ETFs")
    
    processed_etfs = load_progress()
    all_etf_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_etf, symbol, processed_etfs): symbol for symbol in us_etfs}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
            symbol = future_to_symbol[future]
            try:
                data = future.result()
                if data:
                    all_etf_data.append(data)
            except Exception as exc:
                print(f'{symbol} generated an exception: {exc}')
            
            if i % 100 == 0:
                print(f"Processed {i} ETFs. Saving intermediate results...")
                save_all_text(all_etf_data, f"data/etf_data_intermediate_{i}.txt")
    
    # ETF 정보와 top 5 보유 종목을 텍스트 파일로 저장
    text_filename = "data/etf_data_final.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_etf_data, text_filename)
    print(f"Saved final ETF data to text file: {text_filename}")

if __name__ == "__main__":
    main()