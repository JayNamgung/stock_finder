# 입력 파일 이름을 지정합니다. 실제 파일 경로로 변경해주세요.
input_file_name = "us_etf_list_240724.txt"

# 출력 파일 이름을 지정합니다.
output_file_name = "extracted_symbols.txt"

# Symbol들을 저장할 리스트를 생성합니다.
symbols = []

# 입력 파일을 열고 읽습니다.
with open(input_file_name, 'r') as file:
    # 첫 줄은 헤더이므로 건너뜁니다.
    next(file)
    
    # 각 줄을 읽습니다.
    for i, line in enumerate(file, 1):
        # 줄을 탭으로 분리하고 첫 번째 항목(Symbol)을 가져옵니다.
        symbol = line.split('\t')[0].strip()
        
        # 빈 문자열이 아니라면 리스트에 추가합니다.
        if symbol:
            symbols.append(symbol)
        
        # 100개 단위로 디버깅 메시지를 출력합니다.
        if i % 100 == 0:
            print(f"현재 {i}개의 라인을 처리했습니다. 추출된 Symbol 수: {len(symbols)}")

# 결과를 새 파일에 저장합니다.
with open(output_file_name, 'w') as output_file:
    for symbol in symbols:
        output_file.write(symbol + '\n')

# 최종 결과를 출력합니다.
print("\n처리 완료!")
print(f"총 추출된 Symbol 수: {len(symbols)}")
print(f"Symbol들이 {output_file_name} 파일에 저장되었습니다.")
print(f"첫 10개 Symbol: {symbols[:10]}")
print(f"마지막 10개 Symbol: {symbols[-10:]}")