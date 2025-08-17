#!/usr/bin/env python3
"""
Анализ полного реестра ПИФ из файла ЦБ РФ
Читает list_PIF.xlsx и показывает структуру данных для планирования анализа всех 96 БПИФ
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_pif_registry():
    """Анализирует файл реестра ПИФ от ЦБ РФ"""
    
    file_path = Path("list_PIF.xlsx")
    
    if not file_path.exists():
        print(f"❌ Файл {file_path} не найден")
        return
    
    try:
        # Читаем Excel файл
        print("📊 Читаем реестр ПИФ от ЦБ РФ...")
        
        # Пробуем разные листы
        xl_file = pd.ExcelFile(file_path)
        print(f"📋 Найдены листы: {xl_file.sheet_names}")
        
        # Читаем первый лист
        df = pd.read_excel(file_path, sheet_name=0)
        
        print(f"\n📈 Общая статистика:")
        print(f"   • Всего записей: {len(df)}")
        print(f"   • Столбцов: {len(df.columns)}")
        
        print(f"\n📋 Структура данных:")
        print(f"   • Столбцы: {list(df.columns)}")
        
        print(f"\n🔍 Первые 5 записей:")
        print(df.head())
        
        print(f"\n📊 Типы данных:")
        print(df.dtypes)
        
        # Ищем БПИФ (Биржевые ПИФ)
        if 'Тип ПИФ' in df.columns or 'Тип' in df.columns:
            type_col = 'Тип ПИФ' if 'Тип ПИФ' in df.columns else 'Тип'
            print(f"\n🏷️ Типы ПИФ:")
            print(df[type_col].value_counts())
            
            # Фильтруем БПИФ
            bpif_mask = df[type_col].str.contains('биржевой|БПИФ|ETF', case=False, na=False)
            bpif_df = df[bpif_mask]
            print(f"\n🎯 Найдено БПИФ: {len(bpif_df)}")
            
            if len(bpif_df) > 0:
                print("\n📋 БПИФ в реестре:")
                for idx, row in bpif_df.iterrows():
                    name_col = None
                    for col in ['Наименование', 'Название', 'Name']:
                        if col in df.columns:
                            name_col = col
                            break
                    
                    if name_col:
                        print(f"   • {row[name_col]}")
        
        # Ищем столбцы с тикерами или кодами
        ticker_cols = [col for col in df.columns if any(word in col.lower() for word in ['тикер', 'код', 'symbol', 'ticker'])]
        if ticker_cols:
            print(f"\n🏷️ Найдены столбцы с тикерами: {ticker_cols}")
            for col in ticker_cols:
                unique_values = df[col].dropna().unique()
                print(f"   • {col}: {len(unique_values)} уникальных значений")
                if len(unique_values) <= 20:
                    print(f"     Значения: {list(unique_values)}")
        
        # Сохраняем в CSV для дальнейшего анализа
        csv_path = "cbr_pif_registry_analysis.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"\n💾 Данные сохранены в {csv_path}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка при чтении файла: {e}")
        return None

if __name__ == "__main__":
    df = analyze_pif_registry()
    
    if df is not None:
        print(f"\n✅ Анализ завершен. Найдено {len(df)} записей в реестре ПИФ")
        print("📝 Следующие шаги:")
        print("   1. Определить какие из записей являются БПИФ")
        print("   2. Извлечь тикеры для торговли на MOEX")
        print("   3. Обновить систему сбора данных для всех найденных фондов")
    else:
        print("❌ Не удалось проанализировать реестр ПИФ")