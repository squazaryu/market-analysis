#!/usr/bin/env python3
"""
Сопоставление БПИФ из реестра ЦБ с тикерами на MOEX
"""

import pandas as pd
import requests
import re
from pathlib import Path
import time

def get_moex_etf_list():
    """Получает список всех ETF с MOEX"""
    
    try:
        # Получаем список всех ETF с MOEX
        url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQTF/securities.json"
        
        print("📊 Получаем список ETF с MOEX...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        securities = data['securities']['data']
        columns = data['securities']['columns']
        
        # Создаем DataFrame
        moex_etf = pd.DataFrame(securities, columns=columns)
        
        print(f"✅ Получено {len(moex_etf)} ETF с MOEX")
        
        return moex_etf
        
    except Exception as e:
        print(f"❌ Ошибка получения данных MOEX: {e}")
        return None

def load_cbr_bpif():
    """Загружает данные БПИФ из реестра ЦБ"""
    
    file_path = Path("bpif_structured_data.csv")
    
    if not file_path.exists():
        print("❌ Файл с БПИФ не найден. Сначала запустите extract_bpif_details.py")
        return None
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"📊 Загружено {len(df)} БПИФ из реестра ЦБ")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки БПИФ: {e}")
        return None

def extract_fund_names(cbr_df):
    """Извлекает названия фондов из данных ЦБ"""
    
    fund_names = []
    
    for idx, row in cbr_df.iterrows():
        fund_info = {
            'cbr_id': row.get('id', idx),
            'registry_number': row.get('Unnamed: 6', ''),
            'short_name': row.get('Unnamed: 4', ''),
            'full_name': row.get('Unnamed: 3', ''),
            'status': row.get('Unnamed: 2', ''),
            'management_company': row.get('Управляющая компания (УК)', ''),
            'formation_date': row.get('Unnamed: 7', ''),
        }
        
        # Извлекаем ключевые слова для поиска
        keywords = []
        
        if fund_info['short_name']:
            # Извлекаем ключевые слова из короткого названия
            name = str(fund_info['short_name']).lower()
            
            # Ищем известные паттерны
            if 'индекс мосбиржи' in name:
                if 'полной доходности' in name:
                    keywords.append('SBMX')
                elif 'государственных облигаций' in name:
                    keywords.append('SBGB')
                elif 'корпоративных облигаций' in name:
                    keywords.append('SBCB')
                elif 'российских ликвидных еврооблигаций' in name:
                    keywords.append('SBRB')
            
            # Ищем по управляющим компаниям
            if 'сбер' in name or 'сбербанк' in name:
                keywords.extend(['SB', 'SBER'])
            elif 'втб' in name:
                keywords.extend(['VTB'])
            elif 'газпромбанк' in name:
                keywords.extend(['GPB'])
            elif 'тинькофф' in name or 'т-капитал' in name:
                keywords.extend(['TINK', 'TCAP'])
            elif 'первая' in name:
                keywords.extend(['FIRST'])
        
        fund_info['search_keywords'] = keywords
        fund_names.append(fund_info)
    
    return fund_names

def match_with_moex(cbr_funds, moex_etf):
    """Сопоставляет БПИФ с тикерами MOEX"""
    
    matches = []
    unmatched_cbr = []
    unmatched_moex = list(moex_etf['SECID'].values)
    
    print(f"\n🔍 Сопоставляем {len(cbr_funds)} БПИФ с {len(moex_etf)} ETF MOEX...")
    
    for fund in cbr_funds:
        matched = False
        best_match = None
        
        # Пропускаем исключенные фонды
        if fund['status'] == 'Исключён из реестра':
            continue
        
        # Поиск по ключевым словам
        for keyword in fund['search_keywords']:
            for _, etf in moex_etf.iterrows():
                ticker = etf['SECID']
                name = str(etf.get('SHORTNAME', '')).lower()
                
                if keyword.lower() in ticker.lower() or keyword.lower() in name:
                    match_info = {
                        'cbr_id': fund['cbr_id'],
                        'cbr_registry_number': fund['registry_number'],
                        'cbr_short_name': fund['short_name'],
                        'cbr_full_name': fund['full_name'],
                        'cbr_management_company': fund['management_company'],
                        'moex_ticker': ticker,
                        'moex_name': etf.get('SHORTNAME', ''),
                        'moex_full_name': etf.get('SECNAME', ''),
                        'match_method': f'keyword: {keyword}',
                        'confidence': 'high' if keyword.lower() == ticker.lower() else 'medium'
                    }
                    
                    matches.append(match_info)
                    matched = True
                    
                    if ticker in unmatched_moex:
                        unmatched_moex.remove(ticker)
                    
                    break
            
            if matched:
                break
        
        # Поиск по названиям (fuzzy matching)
        if not matched:
            fund_name = str(fund['short_name']).lower()
            
            for _, etf in moex_etf.iterrows():
                ticker = etf['SECID']
                etf_name = str(etf.get('SHORTNAME', '')).lower()
                
                # Простое сравнение по общим словам
                fund_words = set(re.findall(r'\b\w+\b', fund_name))
                etf_words = set(re.findall(r'\b\w+\b', etf_name))
                
                common_words = fund_words.intersection(etf_words)
                
                if len(common_words) >= 2:  # Минимум 2 общих слова
                    match_info = {
                        'cbr_id': fund['cbr_id'],
                        'cbr_registry_number': fund['registry_number'],
                        'cbr_short_name': fund['short_name'],
                        'cbr_full_name': fund['full_name'],
                        'cbr_management_company': fund['management_company'],
                        'moex_ticker': ticker,
                        'moex_name': etf.get('SHORTNAME', ''),
                        'moex_full_name': etf.get('SECNAME', ''),
                        'match_method': f'name similarity: {common_words}',
                        'confidence': 'low'
                    }
                    
                    matches.append(match_info)
                    matched = True
                    
                    if ticker in unmatched_moex:
                        unmatched_moex.remove(ticker)
                    
                    break
        
        if not matched:
            unmatched_cbr.append(fund)
    
    return matches, unmatched_cbr, unmatched_moex

def main():
    """Основная функция"""
    
    # Загружаем данные
    cbr_df = load_cbr_bpif()
    if cbr_df is None:
        return
    
    moex_etf = get_moex_etf_list()
    if moex_etf is None:
        return
    
    # Извлекаем названия фондов
    cbr_funds = extract_fund_names(cbr_df)
    
    # Сопоставляем
    matches, unmatched_cbr, unmatched_moex = match_with_moex(cbr_funds, moex_etf)
    
    # Результаты
    print(f"\n📊 Результаты сопоставления:")
    print(f"   • Найдено совпадений: {len(matches)}")
    print(f"   • БПИФ без совпадений: {len(unmatched_cbr)}")
    print(f"   • ETF MOEX без совпадений: {len(unmatched_moex)}")
    
    # Сохраняем совпадения
    if matches:
        matches_df = pd.DataFrame(matches)
        matches_file = "bpif_moex_matches.csv"
        matches_df.to_csv(matches_file, index=False, encoding='utf-8')
        print(f"\n💾 Совпадения сохранены в {matches_file}")
        
        print(f"\n🎯 Найденные совпадения:")
        for match in matches[:10]:  # Показываем первые 10
            print(f"   • {match['moex_ticker']}: {match['cbr_short_name'][:60]}...")
        
        if len(matches) > 10:
            print(f"   ... и еще {len(matches) - 10} совпадений")
    
    # Сохраняем несовпавшие БПИФ
    if unmatched_cbr:
        unmatched_df = pd.DataFrame(unmatched_cbr)
        unmatched_file = "unmatched_bpif.csv"
        unmatched_df.to_csv(unmatched_file, index=False, encoding='utf-8')
        print(f"\n💾 Несовпавшие БПИФ сохранены в {unmatched_file}")
    
    # Показываем несовпавшие ETF MOEX
    if unmatched_moex:
        print(f"\n📋 ETF MOEX без совпадений в реестре ЦБ:")
        moex_unmatched = moex_etf[moex_etf['SECID'].isin(unmatched_moex)]
        for _, etf in moex_unmatched.head(10).iterrows():
            print(f"   • {etf['SECID']}: {etf.get('SHORTNAME', 'Без названия')}")
        
        if len(unmatched_moex) > 10:
            print(f"   ... и еще {len(unmatched_moex) - 10} ETF")
    
    print(f"\n✅ Анализ завершен")
    print(f"📝 Следующие шаги:")
    print(f"   1. Проверьте файл bpif_moex_matches.csv")
    print(f"   2. Обновите систему сбора данных для всех найденных тикеров")
    print(f"   3. Проведите анализ всех {len(matches)} БПИФ")

if __name__ == "__main__":
    main()