import yfinance as yf
import json
import os
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_etf_list(country='US'):
    if country == 'US':
        url = "https://finance.yahoo.com/etfs"
    elif country == 'KR':
        url = "https://finance.yahoo.com/screener/predefined/korean_etfs"
    else:
        raise ValueError("Unsupported country. Use 'US' or 'KR'.")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        
        df = pd.read_html(response.text)[0]
        etfs = df['Symbol'].tolist()
        return etfs
    except Exception as e:
        print(f"Error fetching ETF list for {country}: {e}")
        return []

def get_top_holdings(symbol):
    url = f"https://finance.yahoo.com/quote/{symbol}/holdings"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        holdings_table = soup.find('table', {'class': 'W(100%) M(0) BdB Bdc($seperatorColor)'})
        if holdings_table:
            holdings = []
            rows = holdings_table.find_all('tr')
            for row in rows[1:6]:  # Get top 5 holdings
                cols = row.find_all('td')
                if len(cols) >= 2:
                    holdings.append({
                        'name': cols[0].text.strip(),
                        'percent': cols[1].text.strip()
                    })
            return holdings
    except Exception as e:
        print(f"Error fetching top holdings for {symbol}: {str(e)}")
    return []

def get_etf_data(symbol, period="1y"):
    try:
        etf = yf.Ticker(symbol)
        
        # ETF 정보
        info = etf.info
        
        # 주가 데이터
        history = etf.history(period=period)
        
        # Top 5 보유 종목
        top_holdings = get_top_holdings(symbol)
        
        return {
            "symbol": symbol,
            "info": info,
            "history": history.reset_index().to_dict(orient="records"),
            "top_holdings": top_holdings
        }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
        return None

def save_all_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

def save_all_text(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for etf in data:
            symbol = etf['symbol']
            description = etf['info'].get('longBusinessSummary', 'No description available.')
            f.write(f"Symbol: {symbol}\n\nDescription:\n{description}\n\n")
            f.write("Top 5 Holdings:\n")
            for holding in etf['top_holdings']:
                f.write(f"- {holding['name']}: {holding['percent']}\n")
            f.write('\n' + '='*50 + '\n\n')

def main():
    print("Fetching US ETF list...")
    us_etfs = get_etf_list('US')
    print(f"Found {len(us_etfs)} US ETFs")
    
    print("Fetching Korean ETF list...")
    kr_etfs = get_etf_list('KR')
    print(f"Found {len(kr_etfs)} Korean ETFs")
    
    all_etfs = us_etfs + kr_etfs
    all_etf_data = []
    
    for symbol in all_etfs:
        print(f"Fetching data for {symbol}...")
        data = get_etf_data(symbol)
        
        if data:
            all_etf_data.append(data)
        
        print(f"Completed processing {symbol}\n")
        
        # API 사용량 제한을 위한 대기
        time.sleep(2)
    
    # 모든 ETF 데이터를 하나의 JSON 파일로 저장
    json_filename = "data/all_etf_data.json"
    os.makedirs(os.path.dirname(json_filename), exist_ok=True)
    save_all_json(all_etf_data, json_filename)
    print(f"Saved all ETF data to JSON file: {json_filename}")
    
    # ETF 설명과 top 5 보유 종목을 텍스트 파일로 저장
    text_filename = "data/etf_descriptions_and_holdings.txt"
    save_all_text(all_etf_data, text_filename)
    print(f"Saved ETF descriptions and top holdings to text file: {text_filename}")

if __name__ == "__main__":
    main()