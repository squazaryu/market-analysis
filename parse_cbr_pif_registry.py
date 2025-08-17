#!/usr/bin/env python3
"""
Парсинг реестра ПИФ ЦБ РФ с правильной обработкой заголовков
"""

import pandas as pd
import numpy as np
from pathlib import Path

def parse_pif_registry():
    """Парсит файл реестра ПИФ с правильной обработкой заголовков"""
    
    file_path = Path("list_PIF.xlsx")
    
    try:
        # Читаем файл, пропуская первые строки до заголовков
        print("📊 Анализируем структуру файла...")
        
        # Читаем первые 10 строк для поиска заголовков
        df_preview = pd.read_excel(file_path, sheet_name=0, nrows=10)
        
        # Ищем строку с заголовками (обычно содержит "№ п/п")
        header_row = None
        for idx, row in df_preview.iterrows():
            if any("№ п/п" in str(cell) for cell in row.values if pd.notna(cell)):
                header_row = idx
                break
        
        if header_row is None:
            print("❌ Не найдена строка с заголовками")
            return None
        
        print(f"📋 Найдена строка заголовков: {header_row}")
        
        # Читаем файл с правильными заголовками
        df = pd.read_excel(file_path, sheet_name=0, header=header_row)
        
        # Очищаем данные
        df = df.dropna(how='all')  # Удаляем пустые строки
        df = df.reset_index(drop=True)
        
        print(f"\n📈 Статистика после очистки:")
        print(f"   • Записей: {len(df)}")
        print(f"   • Столбцов: {len(df.columns)}")
        
        print(f"\n📋 Столбцы:")
        for i, col in enumerate(df.columns):
            print(f"   {i+1:2d}. {col}")
        
        # Ищем БПИФ
        type_columns = [col for col in df.columns if any(word in str(col).lower() for word in ['тип', 'вид', 'категория'])]
        
        if type_columns:
            print(f"\n🏷️ Найдены столбцы с типами ПИФ: {type_columns}")
            
            for col in type_columns:
                print(f"\n📊 Значения в столбце '{col}':")
                values = df[col].value_counts().head(10)
                for val, count in values.items():
                    print(f"   • {val}: {count}")
                
                # Ищем БПИФ
                bpif_mask = df[col].astype(str).str.contains('биржевой|БПИФ|ETF|exchange', case=False, na=False)
                bpif_count = bpif_mask.sum()
                print(f"   🎯 БПИФ в этом столбце: {bpif_count}")
                
                if bpif_count > 0:
                    bpif_df = df[bpif_mask]
                    print(f"\n📋 Найденные БПИФ:")
                    
                    # Ищем столбец с названиями
                    name_cols = [c for c in df.columns if any(word in str(c).lower() for word in ['наименование', 'название', 'name'])]
                    
                    if name_cols:
                        name_col = name_cols[0]
                        print(f"   Используем столбец названий: '{name_col}'")
                        
                        for idx, row in bpif_df.head(20).iterrows():
                            name = row[name_col] if pd.notna(row[name_col]) else "Без названия"
                            print(f"   • {name}")
                        
                        if len(bpif_df) > 20:
                            print(f"   ... и еще {len(bpif_df) - 20} БПИФ")
                    
                    # Сохраняем БПИФ в отдельный файл
                    bpif_file = "cbr_bpif_list.csv"
                    bpif_df.to_csv(bpif_file, index=False, encoding='utf-8')
                    print(f"\n💾 БПИФ сохранены в {bpif_file}")
                    
                    return bpif_df
        
        # Если не нашли по типу, ищем по названиям
        name_cols = [c for c in df.columns if any(word in str(c).lower() for word in ['наименование', 'название', 'name'])]
        
        if name_cols:
            print(f"\n🔍 Поиск БПИФ по названиям в столбцах: {name_cols}")
            
            for name_col in name_cols:
                bpif_mask = df[name_col].astype(str).str.contains('биржевой|БПИФ|ETF|exchange', case=False, na=False)
                bpif_count = bpif_mask.sum()
                
                if bpif_count > 0:
                    print(f"   🎯 Найдено БПИФ по названию в '{name_col}': {bpif_count}")
                    bpif_df = df[bpif_mask]
                    
                    print(f"\n📋 БПИФ по названиям:")
                    for idx, row in bpif_df.head(20).iterrows():
                        name = row[name_col] if pd.notna(row[name_col]) else "Без названия"
                        print(f"   • {name}")
                    
                    if len(bpif_df) > 20:
                        print(f"   ... и еще {len(bpif_df) - 20} БПИФ")
                    
                    # Сохраняем
                    bpif_file = "cbr_bpif_by_name.csv"
                    bpif_df.to_csv(bpif_file, index=False, encoding='utf-8')
                    print(f"\n💾 БПИФ сохранены в {bpif_file}")
                    
                    return bpif_df
        
        # Показываем образец данных для ручного анализа
        print(f"\n📋 Образец данных (первые 5 строк):")
        print(df.head())
        
        # Сохраняем полный файл
        full_file = "cbr_full_registry.csv"
        df.to_csv(full_file, index=False, encoding='utf-8')
        print(f"\n💾 Полный реестр сохранен в {full_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    df = parse_pif_registry()
    
    if df is not None:
        print(f"\n✅ Парсинг завершен")
        print("📝 Рекомендации:")
        print("   1. Проверьте сохраненные CSV файлы")
        print("   2. Найдите столбцы с тикерами MOEX")
        print("   3. Обновите систему для анализа всех БПИФ")