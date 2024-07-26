import os
import time
import pandas as pd
import random
import json
import concurrent.futures
from deep_translator import GoogleTranslator
import yfinance as yf
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("stock_scraper.log"), logging.StreamHandler()])

# 사용자가 원하는 stock 개수를 지정할 수 있는 전역 변수
STOCK_COUNT = 5567  # 원하는 stock 수로 설정
MAX_WORKERS = 10  # 동시에 실행할 최대 worker 수

# Google Translate 객체 생성
translator = GoogleTranslator(source='en', target='ko')

def get_us_stock_list(filename, limit):
    with open(filename, 'r') as file:
        stocks = [line.strip() for line in file if line.strip()]
    return stocks[:limit]

def translate_to_korean(text):
    try:
        return translator.translate(text)
    except Exception as e:
        logging.error(f"Translation error: {str(e)}")
        return f"[번역 실패: {str(e)}] " + text

def truncate_to_last_sentence(text, max_length=1000):
    if len(text) <= max_length:
        return text
    truncated = text[:max_length]
    last_period = truncated.rfind('.')
    if last_period != -1:
        return truncated[:last_period + 1]
    return truncated

def safe_get(dictionary, key, default=""):
    value = dictionary.get(key, default)
    if value in ["N/A", "None", None]:
        return default
    return value

def get_financial_data(ticker):
    try:
        income_stmt = ticker.financials
        balance_sheet = ticker.balance_sheet
        
        if not income_stmt.empty and not balance_sheet.empty:
            latest_income = income_stmt.iloc[:, 0]
            latest_balance = balance_sheet.iloc[:, 0]
            
            items = ["매출액", "영업이익", "순이익", "총자산", "총부채", "총자본"]
            values = [
                safe_get(latest_income, "Total Revenue", 0),
                safe_get(latest_income, "Operating Income", 0),
                safe_get(latest_income, "Net Income", 0),
                safe_get(latest_balance, "Total Assets", 0),
                safe_get(latest_balance, "Total Liabilities Net Minority Interest", 0),
                safe_get(latest_balance, "Total Stockholder Equity", 0)
            ]
            
            return {
                "항목": items,
                "값": values
            }
    except Exception as e:
        logging.error(f"Error in get_financial_data: {str(e)}")
    return {"항목": [], "값": []}

def get_stock_data(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            original_summary = safe_get(info, "longBusinessSummary", "No description available.")
            translated_summary = translate_to_korean(original_summary)
            
            financials = get_financial_data(ticker)

            return {
                "info": {
                    "symbol": safe_get(info, "symbol", symbol),
                    "longName": safe_get(info, "longName"),
                    "sector": safe_get(info, "sector"),
                    "industry": safe_get(info, "industry"),
                    "category": safe_get(info, "industry"),  # Using industry as category if not available
                    "longBusinessSummary": translated_summary,
                    "financials": financials
                }
            }
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
            else:
                logging.error(f"Max retries reached for {symbol}")
                return None

def save_all_text(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for stock in data:
            info = stock.get('info', {})
            content = f"티커: {info.get('symbol', '')}\n"
            content += f"이름: {info.get('longName', '')}\n"
            content += f"섹터: {info.get('sector', '')}\n"
            content += f"산업: {info.get('industry', '')}\n"
            content += f"카테고리: {info.get('category', '')}\n"
            content += "\n재무제표 정보 (최근 1년):\n"
            financials = info.get('financials', {"항목": [], "값": []})
            for item, value in zip(financials["항목"], financials["값"]):
                content += f"{item}: {value:,.0f}\n"
            content += f"\n설명:\n{truncate_to_last_sentence(info.get('longBusinessSummary', ''))}\n\n"
            content += '\n' + '='*50 + '\n\n'
            f.write(content)

def save_progress(processed_stocks, filename='progress.json'):
    with open(filename, 'w') as f:
        json.dump(processed_stocks, f)

def load_progress(filename='progress.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}

def process_stock(symbol, processed_stocks):
    data = get_stock_data(symbol)
    if data:
        processed_stocks[symbol] = data
        save_progress(processed_stocks)
    return data

def generate_natural_language_summary(data):
    summaries = []
    for stock in data:
        info = stock.get('info', {})
        summary = f"{info.get('longName', '')}(티커: {info.get('symbol', '')})은 "
        if info.get('sector'):
            summary += f"{info.get('sector')} 섹터"
        if info.get('industry'):
            summary += f", {info.get('industry')} 산업"
        summary += "에 속하는 주식입니다.\n"
        if info.get('category'):
            summary += f"카테고리: {info.get('category')}\n"
        summary += "\n재무제표 정보 (최근 1년):\n"
        financials = info.get('financials', {"항목": [], "값": []})
        for item, value in zip(financials["항목"], financials["값"]):
            summary += f"{item}: {value:,.0f}\n"
        summary += f"\n이 주식에 대한 설명은 다음과 같습니다.\n{truncate_to_last_sentence(info.get('longBusinessSummary', ''))}\n\n"
        summaries.append(summary)
        summaries.append('=' * 50)
    return "\n\n".join(summaries)

def main():
    symbol_file = "extracted_symbols.txt"
    logging.info(f"Reading {STOCK_COUNT} US stocks from {symbol_file}...")
    us_stocks = get_us_stock_list(symbol_file, STOCK_COUNT)
    logging.info(f"Found {len(us_stocks)} US stocks")
    
    processed_stocks = load_progress()
    all_stock_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_stock, symbol, processed_stocks): symbol for symbol in us_stocks}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
            symbol = future_to_symbol[future]
            try:
                data = future.result()
                if data:
                    all_stock_data.append(data)
            except Exception as exc:
                logging.error(f'{symbol} generated an exception: {exc}')
            
            if i % 100 == 0:
                logging.info(f"Processed {i} stocks. Saving intermediate results...")
                save_all_text(all_stock_data, f"data/stock_data_intermediate_korean_translated_{i}.txt")
    
    text_filename = "data/stock_data_korean_translated.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_stock_data, text_filename)
    logging.info(f"Saved final stock data to text file: {text_filename}")
    
    nl_summary = generate_natural_language_summary(all_stock_data)
    nl_filename = "data/stock_data_natural_language_summary.txt"
    with open(nl_filename, 'w', encoding='utf-8') as f:
        f.write(nl_summary)
    logging.info(f"Saved natural language summary to: {nl_filename}")

if __name__ == "__main__":
    main()