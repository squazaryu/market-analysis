#!/usr/bin/env python3
"""
Проверка структуры данных ETF для исправления отчета
"""

from full_moex_etf_collector import FullMOEXETFCollector

def main():
    collector = FullMOEXETFCollector()
    
    # Собираем данные по первым 3 ETF для проверки структуры
    etf_list = collector.get_all_moex_etf()
    sample_etf = etf_list.head(3)
    
    print("📊 Собираем образец данных для проверки структуры...")
    
    sample_data = []
    for idx, etf_info in sample_etf.iterrows():
        ticker = etf_info['ticker']
        print(f"Собираем данные для {ticker}...")
        
        etf_data = collector.etf_collector.collect_etf_data(ticker)
        if etf_data:
            sample_data.append(etf_data)
    
    if sample_data:
        print(f"\n📋 Структура данных ETF:")
        print(f"Ключи в данных: {list(sample_data[0].keys())}")
        
        print(f"\n📊 Пример данных:")
        for key, value in sample_data[0].items():
            print(f"   {key}: {value} ({type(value).__name__})")
    else:
        print("❌ Не удалось получить образец данных")

if __name__ == "__main__":
    main()