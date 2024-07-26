import yfinance as yf

def get_financial_data(symbol):
    try:
        # 주식 정보 가져오기
        ticker = yf.Ticker(symbol)
        
        # 재무제표 정보 가져오기
        income_stmt = ticker.financials
        balance_sheet = ticker.balance_sheet
        
        if not income_stmt.empty and not balance_sheet.empty:
            # 가장 최근 연도의 데이터 가져오기
            latest_income = income_stmt.iloc[:, 0]
            latest_balance = balance_sheet.iloc[:, 0]
            
            financials = {
                "총수익": latest_income.get("Total Revenue", 0),
                "영업이익": latest_income.get("Operating Income", 0),
                "순이익": latest_income.get("Net Income", 0),
                "총자산": latest_balance.get("Total Assets", 0),
                "총부채": latest_balance.get("Total Liabilities Net Minority Interest", 0),
                "총자본": latest_balance.get("Total Stockholder Equity", 0),
            }
            
            # 결과 출력
            print(f"\n{symbol} 재무제표 정보:")
            for key, value in financials.items():
                print(f"{key}: ${value:,.0f}")
        else:
            print(f"\n{symbol}의 재무제표 정보를 가져올 수 없습니다.")
    
    except Exception as e:
        print(f"\n{symbol} 데이터 가져오기 오류: {str(e)}")

# 테스트할 주식 심볼 리스트
symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "MYGN"]

# 각 주식에 대해 재무제표 정보 가져오기
for symbol in symbols:
    get_financial_data(symbol)
    print("\n" + "="*50)

print("\n테스트 완료")