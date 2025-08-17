#!/usr/bin/env python3
"""
Извлечение детальной информации о БПИФ из реестра ЦБ
"""

import pandas as pd
import re
from pathlib import Path

def extract_bpif_details():
    """Извлекает детальную информацию о БПИФ"""
    
    # Читаем сохраненный файл с БПИФ
    bpif_file = Path("cbr_bpif_by_name.csv")
    
    if not bpif_file.exists():
        print("❌ Файл с БПИФ не найден. Сначала запустите parse_cbr_pif_registry.py")
        return None
    
    try:
        df = pd.read_csv(bpif_file, encoding='utf-8')
        print(f"📊 Загружено {len(df)} БПИФ из файла")
        
        # Анализируем структуру
        print(f"\n📋 Структура данных БПИФ:")
        for i, col in enumerate(df.columns):
            non_null_count = df[col].notna().sum()
            print(f"   {i+1:2d}. {col} (заполнено: {non_null_count}/{len(df)})")
        
        # Ищем столбцы с полезной информацией
        print(f"\n🔍 Анализ содержимого столбцов:")
        
        # Проверяем каждый столбец на наличие полезной информации
        useful_columns = {}
        
        for col in df.columns:
            if col.startswith('Unnamed'):
                # Смотрим на уникальные значения
                unique_vals = df[col].dropna().unique()
                
                if len(unique_vals) > 0 and len(unique_vals) < len(df):
                    sample_vals = list(unique_vals[:5])
                    print(f"\n   📋 {col}:")
                    print(f"      Уникальных значений: {len(unique_vals)}")
                    print(f"      Примеры: {sample_vals}")
                    
                    # Определяем тип содержимого
                    content_type = identify_content_type(unique_vals)
                    if content_type:
                        print(f"      Тип содержимого: {content_type}")
                        useful_columns[col] = content_type
        
        # Создаем структурированную таблицу БПИФ
        print(f"\n📊 Создаем структурированную таблицу БПИФ...")
        
        structured_data = []
        
        for idx, row in df.iterrows():
            bpif_info = {
                'id': idx + 1,
                'raw_data': {}
            }
            
            # Собираем все непустые данные
            for col in df.columns:
                if pd.notna(row[col]) and str(row[col]).strip():
                    bpif_info['raw_data'][col] = str(row[col]).strip()
            
            structured_data.append(bpif_info)
        
        # Пытаемся найти тикеры и названия
        print(f"\n🎯 Поиск тикеров и названий...")
        
        tickers_found = []
        names_found = []
        
        for item in structured_data[:10]:  # Анализируем первые 10 для примера
            print(f"\n📋 БПИФ #{item['id']}:")
            
            potential_ticker = None
            potential_name = None
            
            for col, value in item['raw_data'].items():
                print(f"   {col}: {value}")
                
                # Ищем тикеры (обычно короткие коды из букв)
                if re.match(r'^[A-Z]{3,6}$', value):
                    potential_ticker = value
                
                # Ищем полные названия (длинные строки с описанием)
                if len(value) > 20 and any(word in value.lower() for word in ['фонд', 'паевой', 'инвестиционный']):
                    potential_name = value
            
            if potential_ticker:
                tickers_found.append(potential_ticker)
                print(f"   🎯 Возможный тикер: {potential_ticker}")
            
            if potential_name:
                names_found.append(potential_name)
                print(f"   📝 Возможное название: {potential_name[:100]}...")
        
        print(f"\n📊 Результаты анализа:")
        print(f"   • Всего БПИФ в реестре: {len(structured_data)}")
        print(f"   • Найдено потенциальных тикеров: {len(set(tickers_found))}")
        print(f"   • Найдено названий: {len(set(names_found))}")
        
        if tickers_found:
            print(f"\n🏷️ Найденные тикеры: {list(set(tickers_found))}")
        
        # Сохраняем структурированные данные
        output_file = "bpif_structured_data.csv"
        
        # Создаем DataFrame для сохранения
        output_rows = []
        for item in structured_data:
            row = {'id': item['id']}
            for col, value in item['raw_data'].items():
                row[col] = value
            output_rows.append(row)
        
        output_df = pd.DataFrame(output_rows)
        output_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\n💾 Структурированные данные сохранены в {output_file}")
        
        return structured_data
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None

def identify_content_type(values):
    """Определяет тип содержимого по значениям"""
    
    if values is None or len(values) == 0:
        return None
    
    sample = [str(v) for v in values[:10] if pd.notna(v)]
    
    if not sample:
        return None
    
    # Проверяем на тикеры (короткие коды из букв)
    if all(re.match(r'^[A-Z]{3,6}$', s) for s in sample):
        return "Возможные тикеры"
    
    # Проверяем на названия фондов
    if any(len(s) > 20 and any(word in s.lower() for word in ['фонд', 'паевой', 'инвестиционный']) for s in sample):
        return "Названия фондов"
    
    # Проверяем на даты
    if any(re.match(r'\d{2}\.\d{2}\.\d{4}', s) for s in sample):
        return "Даты"
    
    # Проверяем на числа
    if all(re.match(r'^\d+(\.\d+)?$', s) for s in sample):
        return "Числовые значения"
    
    # Проверяем на коды/номера
    if all(len(s) < 20 and re.match(r'^[A-Z0-9\-]+$', s) for s in sample):
        return "Коды/номера"
    
    return "Текстовые данные"

if __name__ == "__main__":
    data = extract_bpif_details()
    
    if data:
        print(f"\n✅ Анализ завершен")
        print("📝 Следующие шаги:")
        print("   1. Проверьте найденные тикеры")
        print("   2. Сопоставьте с данными MOEX")
        print("   3. Обновите систему сбора данных")