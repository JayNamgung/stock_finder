def check_section_lengths(file_path, delimiter='==================================================', min_length=1000):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    sections = content.split(delimiter)
    
    for i, section in enumerate(sections, 1):
        section = section.strip()
        if section:  # 빈 섹션 무시
            length = len(section)
            status = "초과" if length > min_length else "미만"
            
            # 티커 정보 찾기
            ticker = "N/A"
            for line in section.split('\n'):
                if line.startswith("티커:"):
                    ticker = line.split(":", 1)[1].strip()
                    break
            
            print(f"섹션 {i}: 길이 {length}자 ({status}), 티커: {ticker}")



file_path = 'us_stock/data/stock_data_korean_translated_240726.txt'
check_section_lengths(file_path)