def check_section_lengths(file_path, delimiter='==================================================', min_length=1000):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    sections = content.split(delimiter)
    
    for i, section in enumerate(sections, 1):
        section = section.strip()
        if section:  # 빈 섹션 무시
            length = len(section)
            status = "초과" if length > min_length else "미만"
            print(f"섹션 {i}: {length}자 ({status})")


file_path = 'us_stock/data/stock_data_korean_translated_240726.txt'
check_section_lengths(file_path)