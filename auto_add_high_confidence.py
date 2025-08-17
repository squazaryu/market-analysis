#!/usr/bin/env python3
"""
Автоматически добавляет фонды с очень высокой уверенностью из группы "требует проверки"
"""

import pandas as pd
import json
from pathlib import Path
from investfunds_parser import InvestFundsParser
import re

def auto_add_high_confidence_funds(min_confidence=0.75):
    """Автоматически добавляет фонды с высокой уверенностью"""
    
    # Загружаем результаты
    try:
        results_df = pd.read_csv('fund_mapping_results.csv')
    except Exception as e:
        print(f"❌ Ошибка загрузки результатов: {e}")
        return
    
    # Фильтруем по уверенности
    high_confidence = results_df[
        (results_df['status'] == 'needs_review') & 
        (results_df['confidence'] >= min_confidence)
    ]
    
    if high_confidence.empty:
        print(f"❌ Нет фондов с уверенностью >= {min_confidence}")
        return
    
    # Загружаем существующий маппинг
    try:
        with open('auto_fund_mapping.json', 'r', encoding='utf-8') as f:
            mapping_data = json.load(f)
            current_mapping = mapping_data.get('mapping', {})
    except:
        current_mapping = {}
    
    print(f"🤖 Автоматическое добавление фондов с уверенностью >= {min_confidence}")
    print(f"📋 Найдено кандидатов: {len(high_confidence)}")
    
    added_count = 0
    verified_count = 0
    
    parser = InvestFundsParser()
    
    for _, row in high_confidence.iterrows():
        ticker = row['ticker']
        fund_id = row['best_match_id']
        confidence = row['confidence']
        found_name = row['best_match_name']
        
        print(f"\n🔍 Проверяем {ticker} (ID: {fund_id}, уверенность: {confidence:.2f})")
        
        # Быстрая проверка - получаем данные
        try:
            fund_data = parser.get_fund_data(fund_id, use_cache=False)
            
            if fund_data and fund_data.get('nav', 0) > 0:
                nav_billions = fund_data['nav'] / 1e9
                unit_price = fund_data['unit_price']
                
                print(f"   📊 СЧА: {nav_billions:.2f} млрд руб., Цена пая: {unit_price:.4f}")
                
                # Дополнительная проверка - тикер в названии или высокая схожесть
                name_contains_ticker = ticker.upper() in found_name.upper()
                
                if name_contains_ticker or confidence >= 0.85:
                    current_mapping[ticker] = fund_id
                    added_count += 1
                    print(f"   ✅ Автоматически добавлен: {ticker} -> {fund_id}")
                else:
                    print(f"   ⚠️  Требует ручной проверки (тикер не найден в названии)")
                
                verified_count += 1
            else:
                print(f"   ❌ Нет данных или нулевая СЧА")
                
        except Exception as e:
            print(f"   ⚠️  Ошибка проверки: {e}")
    
    # Сохраняем обновленный маппинг
    if added_count > 0:
        updated_data = {
            'mapping': current_mapping,
            'stats': {
                'total_mapped': len(current_mapping),
                'auto_added_high_conf': added_count,
                'verified_funds': verified_count
            },
            'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open('auto_fund_mapping.json', 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, indent=2, ensure_ascii=False)
        
        # Обновляем investfunds_parser.py
        update_parser_mapping(current_mapping)
        
        print(f"\n✅ Результаты:")
        print(f"   🤖 Автоматически добавлено: {added_count}")
        print(f"   🔍 Проверено: {verified_count}")
        print(f"   📊 Всего замаплено: {len(current_mapping)}/96 = {len(current_mapping)/96*100:.1f}%")
    else:
        print(f"\n📝 Нет фондов для автоматического добавления")

def update_parser_mapping(mapping):
    """Обновляет маппинг в investfunds_parser.py"""
    
    parser_file = Path('investfunds_parser.py')
    
    if not parser_file.exists():
        print("❌ Файл investfunds_parser.py не найден")
        return
    
    # Читаем файл
    with open(parser_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Создаем новый маппинг с комментариями
    mapping_lines = []
    mapping_lines.append("        # Маппинг тикеров на ID фондов (автоматически обновлен)")
    mapping_lines.append("        self.fund_mapping = {")
    
    for ticker, fund_id in sorted(mapping.items()):
        mapping_lines.append(f"            '{ticker}': {fund_id},")
    
    mapping_lines.append("        }")
    
    new_mapping_text = "\n".join(mapping_lines)
    
    # Заменяем старый маппинг
    pattern = r'# Маппинг тикеров на ID фондов.*?self\.fund_mapping = \{[^}]*\}'
    
    new_content = re.sub(
        pattern,
        new_mapping_text,
        content,
        flags=re.DOTALL
    )
    
    # Сохраняем файл
    with open(parser_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"🔄 Файл investfunds_parser.py обновлен с {len(mapping)} фондами")

if __name__ == "__main__":
    auto_add_high_confidence_funds(min_confidence=0.75)