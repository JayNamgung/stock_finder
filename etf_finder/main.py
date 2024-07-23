import yfinance as yf
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

def get_top_holdings(symbol):
    url = f"https://seekingalpha.com/symbol/{symbol}/holdings"
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print(f"Fetching holdings for {symbol} from URL: {url}")
        driver.get(url)
        
        # Wait for the table to load
        wait = WebDriverWait(driver, 10)
        table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "holdings")))
        
        print("Found holdings table")
        
        holdings = []
        rows = table.find_elements(By.TAG_NAME, "tr")[1:6]  # Get top 5 holdings, skip header
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                holding = {
                    'name': cols[1].text.strip(),
                    'symbol': cols[0].text.strip(),
                    'percent': cols[2].text.strip()
                }
                holdings.append(holding)
                print(f"Added holding: {holding}")
        
        return holdings
    except Exception as e:
        print(f"Error fetching top holdings for {symbol}: {str(e)}")
    finally:
        driver.quit()
    
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
    print("Fetching 5 US ETFs...")
    us_etfs = get_us_etf_list(5)
    print(f"Found {len(us_etfs)} US ETFs")
    
    all_etf_data = []
    
    for symbol, _ in us_etfs:
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