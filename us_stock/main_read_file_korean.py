import yfinance as yf
import os
import time
import pandas as pd
import random
import json
import concurrent.futures
from deep_translator import GoogleTranslator

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
        print(f"Attempting to translate: {text[:50]}...")  # 번역 시도 로그
        translated = translator.translate(text)
        print(f"Translation result: {translated[:50]}...")  # 번역 결과 로그
        return translated
    except Exception as e:
        print(f"Translation error: {str(e)}")
        return f"[번역 실패: {str(e)}] " + text  # 번역 실패 시 오류 메시지와 함께 원본 텍스트 반환

def get_stock_data(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(symbol)
            
            info = stock.info
            original_summary = info.get("longBusinessSummary", "No description available.")
            translated_summary = translate_to_korean(original_summary)
            
            # 섹터, 산업, 테마 정보 가져오기
            sector = info.get("sector", "N/A")
            industry = info.get("industry", "N/A")
            category = info.get("category", "N/A")
            
            # 추가 정보 가져오기
            business_summary = info.get("longBusinessSummary", "N/A")
            market_cap = info.get("marketCap", "N/A")
            
            required_info = {
                "symbol": info.get("symbol", symbol),
                "longName": info.get("longName", "N/A"),
                "sector": sector,
                "industry": industry,
                "category": category,
                "longBusinessSummary": translated_summary,
                "marketCap": market_cap
            }
            
            return {
                "info": required_info
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
        for stock in data:
            info = stock.get('info', {})
            content = f"티커: {info.get('symbol', 'N/A')}\n"
            content += f"이름: {info.get('longName', 'N/A')}\n"
            content += f"섹터: {info.get('sector', 'N/A')}\n"
            content += f"산업: {info.get('industry', 'N/A')}\n"
            content += f"카테고리: {info.get('category', 'N/A')}\n"
            content += f"시가총액: {info.get('marketCap', 'N/A')}\n"
            content += f"\n설명:\n{info.get('longBusinessSummary', 'N/A')}\n\n"
            
            if len(content) > 1000:
                content += "\n[참고 : 1000 글자가 넘는 내용입니다.]\n"
            
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
        print(f"Skipping already processed stock: {symbol}")
        return processed_stocks[symbol]

    print(f"Fetching data for {symbol}...")
    data = get_stock_data(symbol)
    
    if data:
        processed_stocks[symbol] = data
        save_progress(processed_stocks)
    
    print(f"Completed processing {symbol}\n")
    return data

def generate_natural_language_summary(data):
    summaries = []
    for stock in data:
        info = stock.get('info', {})
        summary = f"{info.get('longName', 'N/A')}(티커: {info.get('symbol', 'N/A')})은 "
        summary += f"{info.get('sector', 'N/A')} 섹터, {info.get('industry', 'N/A')} 산업에 속하는 주식입니다.\n"
        if info.get('category', 'N/A') != 'N/A':
            summary += f"카테고리: {info.get('category', 'N/A')}\n"
        summary += f"시가총액: {info.get('marketCap', 'N/A')}\n"
        summary += f"이 주식에 대한 설명은 다음과 같습니다.\n{info.get('longBusinessSummary', 'N/A')}\n\n"
        
        summaries.append(summary)
        summaries.append('=' * 50)  # 50개의 '=' 문자로 구분선 추가
    
    return "\n\n".join(summaries)

def main():
    symbol_file = "extracted_symbols.txt"  # 추출된 symbol 파일 이름
    print(f"Reading {STOCK_COUNT} US stocks from {symbol_file}...")
    us_stocks = get_us_stock_list(symbol_file, STOCK_COUNT)
    print(f"Found {len(us_stocks)} US stocks")
    
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
                print(f'{symbol} generated an exception: {exc}')
            
            if i % 100 == 0:
                print(f"Processed {i} stocks. Saving intermediate results...")
                save_all_text(all_stock_data, f"data/stock_data_intermediate_korean_translated_{i}.txt")
    
    # stock 정보를 텍스트 파일로 저장
    text_filename = "data/stock_data_korean_translated.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_stock_data, text_filename)
    print(f"Saved final stock data to text file: {text_filename}")
    
    # 자연어 처리된 결과물 생성 및 저장
    nl_summary = generate_natural_language_summary(all_stock_data)
    nl_filename = "data/stock_data_natural_language_summary.txt"
    with open(nl_filename, 'w', encoding='utf-8') as f:
        f.write(nl_summary)
    print(f"Saved natural language summary to: {nl_filename}")

if __name__ == "__main__":
    main()