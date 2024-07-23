import yfinance as yf
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO

def get_us_etf_list(limit=10):
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
                volume = cols[6].text.strip().replace(',', '')  # Volume is typically in the 7th column
                try:
                    volume = int(volume)
                except ValueError:
                    volume = 0
                etfs.append((symbol, volume, 'US'))
    
    return etfs

def get_kr_etf_list(limit=10):
    gen_otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    gen_otp_data = {
        "mktId": "ETF",
        "trdDd": "20230629",  # You may want to use current date
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT03901"
    }
    otp = requests.post(gen_otp_url, gen_otp_data).text

    down_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    down_data = {"code": otp}
    response = requests.post(down_url, down_data)
    
    df = pd.read_csv(BytesIO(response.content), encoding='euc-kr')
    etfs = []
    for _, row in df.iterrows()[:limit]:  # Only process up to the limit
        symbol = row['종목코드']
        volume = row['거래량']
        etfs.append((f"{symbol}.KS", volume, 'KR'))
    
    return etfs

def get_top_holdings(symbol):
    url = f"https://finance.yahoo.com/quote/{symbol}/holdings"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        holdings = []
        tables = soup.find_all('table')
        for table in tables:
            if table.find('th', text='Holding'):
                rows = table.find_all('tr')
                for row in rows[1:6]:  # Get top 5 holdings
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        holdings.append({
                            'name': cols[0].text.strip(),
                            'percent': cols[1].text.strip()
                        })
                break
        return holdings
    except Exception as e:
        print(f"Error fetching top holdings for {symbol}: {str(e)}")
    return []

def get_etf_data(symbol):
    try:
        etf = yf.Ticker(symbol)
        
        # 필요한 ETF 정보만 추출
        info = etf.info
        required_info = {
            "category": info.get("category", "N/A"),
            "longName": info.get("longName", "N/A"),
            "timeZoneFullName": info.get("timeZoneFullName", "N/A"),
            "symbol": info.get("symbol", symbol),
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
            f.write(f"Long Name: {info['longName']}\n")
            f.write(f"Category: {info['category']}\n")
            f.write(f"Time Zone: {info['timeZoneFullName']}\n")
            f.write(f"\nDescription:\n{info['longBusinessSummary']}\n\n")
            f.write("Top 5 Holdings:\n")
            for holding in etf['top_holdings']:
                f.write(f"- {holding['name']}: {holding['percent']}\n")
            f.write('\n' + '='*50 + '\n\n')

def main():
    print("Fetching 10 US ETFs...")
    us_etfs = get_us_etf_list(10)
    print(f"Found {len(us_etfs)} US ETFs")
    
    print("Fetching 10 Korean ETFs...")
    kr_etfs = get_kr_etf_list(10)
    print(f"Found {len(kr_etfs)} Korean ETFs")
    
    all_etfs = us_etfs + kr_etfs
    
    all_etf_data = []
    
    for symbol, _, _ in all_etfs:
        print(f"Fetching data for {symbol}...")
        data = get_etf_data(symbol)
        
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