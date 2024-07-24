import yfinance as yf
from yahooquery import Ticker
import os
import time
import pandas as pd
import random
import json
import concurrent.futures
from deep_translator import GoogleTranslator

# 사용자가 원하는 ETF 개수를 지정할 수 있는 전역 변수
ETF_COUNT = 3602  # 원하는 ETF 수로 설정
MAX_WORKERS = 10  # 동시에 실행할 최대 worker 수

# Google Translate 객체 생성
translator = GoogleTranslator(source='en', target='ko')

# 파일 read 하여 추출(파일 추출 출처 : https://stockanalysis.com/etf/)
def get_us_etf_list(filename, limit):
    with open(filename, 'r') as file:
        etfs = [line.strip() for line in file if line.strip()]
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

def translate_to_korean(text):
    try:
        print(f"Attempting to translate: {text[:50]}...")  # 번역 시도 로그
        translated = translator.translate(text)
        print(f"Translation result: {translated[:50]}...")  # 번역 결과 로그
        return translated
    except Exception as e:
        print(f"Translation error: {str(e)}")
        return f"[번역 실패: {str(e)}] " + text  # 번역 실패 시 오류 메시지와 함께 원본 텍스트 반환

def get_etf_data(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            etf = yf.Ticker(symbol)
            
            info = etf.info
            original_summary = info.get("longBusinessSummary", "No description available.")
            translated_summary = translate_to_korean(original_summary)
            
            required_info = {
                "symbol": info.get("symbol", symbol),
                "longName": info.get("longName", "N/A"),
                "category": info.get("category", "N/A"),
                "longBusinessSummary": translated_summary
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
            content = f"티커: {info['symbol']}\n"
            content += f"이름: {info['longName']}\n"
            content += f"카테고리: {info['category']}\n"
            content += f"\n설명:\n{info['longBusinessSummary']}\n\n"
            
            if etf['top_holdings']:
                content += "편입종목 상위 5개:\n"
                for holding in etf['top_holdings']:
                    content += f"- {holding['name']} ({holding['symbol']}): {holding['percent']}\n"
            
            if len(content) > 1000:
                content += "\n[참고 : 1000 글자가 넘는 내용입니다.]\n"
            
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

# 새로운 함수: 자연어 처리된 결과물 생성
def generate_natural_language_summary(data):
    summaries = []
    for etf in data:
        info = etf['info']
        summary = f"{info['longName']}(티커: {info['symbol']})은 {info['category']} 카테고리에 속하는 ETF입니다. "
        summary += f"이 ETF에 대한 설명은 다음과 같습니다. {info['longBusinessSummary']}"
        
        if etf['top_holdings']:
            summary += "주요 편입 종목으로는 "
            holdings = [f"{h['name']}({h['percent']})" for h in etf['top_holdings'][:3]]
            summary += ", ".join(holdings) + " 등이 있습니다."
        
        summaries.append(summary)
    
    return "\n\n".join(summaries)

def main():
    symbol_file = "extracted_symbols.txt"  # 추출된 symbol 파일 이름
    print(f"Reading {ETF_COUNT} US ETFs from {symbol_file}...")
    us_etfs = get_us_etf_list(symbol_file, ETF_COUNT)
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
                save_all_text(all_etf_data, f"data/etf_data_intermediate_korean_translated_{i}.txt")
    
    # ETF 정보와 top 5 보유 종목을 텍스트 파일로 저장
    text_filename = "data/etf_data_korean_translated.txt"
    os.makedirs(os.path.dirname(text_filename), exist_ok=True)
    save_all_text(all_etf_data, text_filename)
    print(f"Saved final ETF data to text file: {text_filename}")
    
    # 자연어 처리된 결과물 생성 및 저장
    nl_summary = generate_natural_language_summary(all_etf_data)
    nl_filename = "data/etf_data_natural_language_summary.txt"
    with open(nl_filename, 'w', encoding='utf-8') as f:
        f.write(nl_summary)
    print(f"Saved natural language summary to: {nl_filename}")

if __name__ == "__main__":
    main()