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
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()])

# 사용자가 원하는 stock 개수를 지정할 수 있는 전역 변수
STOCK_COUNT = 5567  # 원하는 stock 수로 설정
MAX_WORKERS = 10  # 동시에 실행할 최대 worker 수

# Google Translate 객체 생성
translator = GoogleTranslator(source='en', target='ko')

# 파일 read 하여 추출(파일 추출 출처 : https://stockanalysis.com/stock/)
def get_us_stock_list(filename, limit):
    with open(filename, 'r') as file:
        stocks = [line.strip() for line in file if line.strip()]
    return stocks[:limit]

def translate_to_korean(text):
    try:
        logging.debug(f"Attempting to translate: {text[:50]}...")  # 번역 시도 로그
        translated = translator.translate(text)
        logging.debug(f"Translation result: {translated[:50]}...")  # 번역 결과 로그
        return translated
    except Exception as e:
        logging.error(f"Translation error: {str(e)}")
        return f"[번역 실패: {str(e)}] " + text  # 번역 실패 시 오류 메시지와 함께 원본 텍스트 반환

def truncate_to_last_sentence(text, max_length=1000):
    if len(text) <= max_length:
        return text
    truncated = text[:max_length]
    last_period = truncated.rfind('.')
    if last_period != -1:
        return truncated[:last_period + 1]
    return truncated

def get_financial_data(ticker):
    logging.debug(f"\nDebugging get_financial_data for ticker: {ticker.ticker}")
    try:
        # 재무제표 정보 가져오기
        logging.debug("Fetching income statement...")
        income_stmt = ticker.financials
        logging.debug(f"Income statement shape: {income_stmt.shape if not income_stmt.empty else 'Empty'}")
        logging.debug(f"Income statement columns: {income_stmt.columns.tolist() if not income_stmt.empty else 'Empty'}")
        
        logging.debug("\nFetching balance sheet...")
        balance_sheet = ticker.balance_sheet
        logging.debug(f"Balance sheet shape: {balance_sheet.shape if not balance_sheet.empty else 'Empty'}")
        logging.debug(f"Balance sheet columns: {balance_sheet.columns.tolist() if not balance_sheet.empty else 'Empty'}")
        
        if not income_stmt.empty and not balance_sheet.empty:
            logging.debug("\nBoth income statement and balance sheet are not empty.")
            # 가장 최근 연도의 데이터 가져오기
            latest_income = income_stmt.iloc[:, 0]
            latest_balance = balance_sheet.iloc[:, 0]
            
            logging.debug("\nExtracting financial data...")
            financials = {
                "매출액": latest_income.get("Total Revenue", 0),
                "영업이익": latest_income.get("Operating Income", 0),
                "순이익": latest_income.get("Net Income", 0),
                "총자산": latest_balance.get("Total Assets", 0),
                "총부채": latest_balance.get("Total Liabilities Net Minority Interest", 0),
                "총자본": latest_balance.get("Total Stockholder Equity", 0),
            }
            
            logging.debug("\nExtracted financial data:")
            for key, value in financials.items():
                logging.debug(f"{key}: {value}")
            
            return financials
        else:
            logging.debug("\nEither income statement or balance sheet is empty.")
            return {}
    except Exception as e:
        logging.error(f"\nError in get_financial_data: {str(e)}", exc_info=True)
        return {}

def get_stock_data(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            logging.debug(f"Attempting to fetch data for {symbol} (Attempt {attempt + 1})")
            ticker = yf.Ticker(symbol)
            
            info = ticker.info
            logging.debug(f"Basic info fetched for {symbol}: {info}")
            
            original_summary = info.get("longBusinessSummary", "No description available.")
            translated_summary = translate_to_korean(original_summary)
            logging.debug(f"Summary translated for {symbol}")
            
            # 섹터, 산업, 테마 정보 가져오기
            sector = info.get("sector", "")
            industry = info.get("industry", "")
            category = info.get("category", "")

            # 'N/A' 또는 None 값을 빈 문자열로 대체
            sector = "" if sector in ["N/A", "None", None] else sector
            industry = "" if industry in ["N/A", "None", None] else industry
            category = "" if category in ["N/A", "None", None] else category

            # 카테고리가 비어있다면 industry를 사용
            if not category:
                category = industry

            # 재무제표 정보 가져오기
            logging.debug(f"Fetching financial data for {symbol}...")
            financials = get_financial_data(ticker)
            logging.debug(f"Financial data fetched for {symbol}: {financials}")

            required_info = {
                "symbol": info.get("symbol", symbol),
                "longName": info.get("longName", "N/A"),
                "sector": sector,
                "industry": industry,
                "category": category,
                "longBusinessSummary": translated_summary,
                "financials": {k: f"{v:,.0f}" for k, v in financials.items() if v != 0}
            }
            
            logging.debug(f"Successfully processed {symbol}: {required_info}")
            return {
                "info": required_info
            }
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}", exc_info=True)
            if attempt < max_retries - 1:
                logging.debug(f"Retrying {symbol} after delay...")
                time.sleep(random.uniform(5, 10))  # Random delay before retrying
            else:
                logging.error(f"Max retries reached for {symbol}")
                return None

def save_all_text(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for stock in data:
            info = stock.get('info', {})
            content = f"티커: {info.get('symbol', 'N/A')}\n"
            content += f"이름: {info.get('longName', 'N/A')}\n"
            content += f"섹터: {info.get('sector', '')}\n"
            content += f"산업: {info.get('industry', '')}\n"
            content += f"카테고리: {info.get('category', '')}\n"
            content += "\n재무제표 정보 (최근 1년):\n"
            financials = info.get('financials', {})
            if financials:
                for key, value in financials.items():
                    content += f"{key}: {value}\n"
            else:
                content += ""  # 재무제표 정보가 없으면 빈 문자열 추가
            content += f"\n설명:\n{truncate_to_last_sentence(info.get('longBusinessSummary', 'N/A'))}\n\n"
            
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
    if symbol in processed_stocks:
        logging.debug(f"Skipping already processed stock: {symbol}")
        return processed_stocks[symbol]

    logging.debug(f"Processing stock: {symbol}")
    data = get_stock_data(symbol)
    
    if data:
        processed_stocks[symbol] = data
        save_progress(processed_stocks)
        logging.debug(f"Completed processing {symbol}")
    else:
        logging.warning(f"Failed to process {symbol}")
    
    return data

def generate_natural_language_summary(data):
    summaries = []
    for stock in data:
        info = stock.get('info', {})
        summary = f"{info.get('longName', 'N/A')}(티커: {info.get('symbol', 'N/A')})은 "
        if info.get('sector', 'N/A') != 'N/A':
            summary += f"{info.get('sector')} 섹터"
        if info.get('industry', 'N/A') != 'N/A':
            summary += f", {info.get('industry')} 산업"
        summary += "에 속하는 주식입니다.\n"
        if info.get('category', 'N/A') != 'N/A':
            summary += f"카테고리: {info.get('category')}\n"
        summary += "\n재무제표 정보 (최근 1년):\n"
        financials = info.get('financials', {})
        if financials:
            for key, value in financials.items():
                summary += f"{key}: {value}\n"
        else:
            summary += ""  # 재무제표 정보가 없으면 빈 문자열 추가
        summary += f"\n이 주식에 대한 설명은 다음과 같습니다.\n{truncate_to_last_sentence(info.get('longBusinessSummary', 'N/A'))}\n\n"
        
        summaries.append(summary)
        summaries.append('=' * 50)  # 50개의 '=' 문자로 구분선 추가
    
    return "\n\n".join(summaries)

def main():
    symbol_file = "extracted_symbols.txt"  # 추출된 symbol 파일 이름
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
                    logging.debug(f"Added data for {symbol} to all_stock_data")
                else:
                    logging.warning(f"No data returned for {symbol}")
            except Exception as exc:
                logging.error(f'{symbol} generated an exception: {exc}', exc_info=True)
            
            if i % 100 == 0:
                logging.info(f"Processed {i} stocks. Saving intermediate results...")
                save_all_text(all_stock_data, f"data/stock_data_intermediate_korean_translated_{i}.txt")
    
    # stock 정보를 텍스트 파일로 저장
    text_filename = "data/stock_data_korean_translated.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_stock_data, text_filename)
    logging.info(f"Saved final stock data to text file: {text_filename}")
    
    # 자연어 처리된 결과물 생성 및 저장
    nl_summary = generate_natural_language_summary(all_stock_data)
    nl_filename = "data/stock_data_natural_language_summary.txt"
    with open(nl_filename, 'w', encoding='utf-8') as f:
        f.write(nl_summary)
    logging.info(f"Saved natural language summary to: {nl_filename}")

if __name__ == "__main__":
    main()