import os
import time
import numpy as np
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

def safe_get(dictionary, key, default=""):
    value = dictionary.get(key, default)
    if value in ["N/A", "None", None, "nan"] or (isinstance(value, float) and np.isnan(value)):
        return default
    return value

def get_financial_data(ticker):
    try:
        income_stmt = ticker.financials
        balance_sheet = ticker.balance_sheet
        cash_flow = ticker.cashflow
        
        if not income_stmt.empty and not balance_sheet.empty and not cash_flow.empty:
            latest_income = income_stmt.iloc[:, 0]
            latest_balance = balance_sheet.iloc[:, 0]
            latest_cash_flow = cash_flow.iloc[:, 0]
            
            financials = {
                "매출액": safe_get(latest_income, "Total Revenue", 0),
                "영업이익": safe_get(latest_income, "Operating Income", 0),
                "순이익": safe_get(latest_income, "Net Income", 0),
                "EBITDA": safe_get(latest_income, "EBITDA", 0),
                "총자산": safe_get(latest_balance, "Total Assets", 0),
                "총부채": safe_get(latest_balance, "Total Liabilities Net Minority Interest", 0),
                "총자본": safe_get(latest_balance, "Total Stockholder Equity", 0),
                "유동자산": safe_get(latest_balance, "Current Assets", 0),
                "유동부채": safe_get(latest_balance, "Current Liabilities", 0),
                "영업활동현금흐름": safe_get(latest_cash_flow, "Operating Cash Flow", 0),
                "투자활동현금흐름": safe_get(latest_cash_flow, "Investing Cash Flow", 0),
                "재무활동현금흐름": safe_get(latest_cash_flow, "Financing Cash Flow", 0),
                "잉여현금흐름": safe_get(latest_cash_flow, "Free Cash Flow", 0),
                "현금및현금성자산": safe_get(latest_balance, "Cash And Cash Equivalents", 0),
            }
            
            # 부채비율과 유동비율 계산 (0으로 나누는 경우 방지)
            total_assets = financials["총자산"]
            current_liabilities = financials["유동부채"]
            if total_assets != 0:
                financials["부채비율"] = (financials["총부채"] / total_assets) * 100
            else:
                financials["부채비율"] = 0
            
            if current_liabilities != 0:
                financials["유동비율"] = (financials["유동자산"] / current_liabilities) * 100
            else:
                financials["유동비율"] = 0
            
            return financials
    except Exception as e:
        logging.error(f"Error in get_financial_data: {str(e)}")
    return {}

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
                    "financials": {k: (f"{v:,.2f}" if v != 0 else "") for k, v in financials.items()}
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
            financials = info.get('financials', {})
            for key, value in financials.items():
                content += f"{key}: {value}\n"
            content += f"\n설명:\n{truncate_to_last_sentence(info.get('longBusinessSummary', ''))}\n\n"
            content += '\n' + '='*50 + '\n\n'
            f.write(content)

def save_progress(processed_etfs, filename='progress.json'):
    with open(filename, 'w') as f:
        json.dump(processed_etfs, f)

def load_progress(filename='progress.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}

def process_stock(symbol, processed_etfs):
    if symbol in processed_etfs:
        print(f"Skipping already processed STOCK: {symbol}")
        return processed_etfs[symbol]

    print(f"Fetching data for {symbol}...")
    data = get_stock_data(symbol)
    
    if data:
        processed_etfs[symbol] = data
        save_progress(processed_etfs)
    
    print(f"Completed processing {symbol}\n")
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
        for key, value in info.get('financials', {}).items():
            summary += f"{key}: {value}\n"
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