#!/usr/bin/env python3
"""
Инструмент для ручной проверки и добавления фондов из группы "требует проверки"
"""

import pandas as pd
import json
from pathlib import Path
from investfunds_parser import InvestFundsParser

def load_review_candidates():
    """Загружает кандидатов для ручной проверки"""
    
    try:
        results_df = pd.read_csv('fund_mapping_results.csv')
        return results_df[results_df['status'] == 'needs_review']
    except Exception as e:
        print(f"❌ Ошибка загрузки файла результатов: {e}")
        return pd.DataFrame()

def verify_fund_manually(ticker, fund_id, expected_name, found_name):
    """Ручная проверка фонда"""
    
    parser = InvestFundsParser()
    
    print(f"\n🔍 Проверяем фонд: {ticker}")
    print(f"📝 Ожидаемое название: {expected_name}")
    print(f"🌐 Найденное название: {found_name}")
    print(f"🆔 Предлагаемый ID: {fund_id}")
    print(f"🔗 URL: https://investfunds.ru/funds/{fund_id}/")
    
    # Пытаемся получить данные
    try:
        fund_data = parser.get_fund_data(fund_id, use_cache=False)
        
        if fund_data:
            print(f"\n📊 Данные с сайта:")
            print(f"   💰 СЧА: {fund_data['nav']/1e9:.2f} млрд руб.")
            print(f"   💵 Цена пая: {fund_data['unit_price']:.4f} руб.")
            print(f"   📅 Дата: {fund_data['date']}")
        else:
            print(f"\n❌ Не удалось получить данные для ID {fund_id}")
            
    except Exception as e:
        print(f"\n⚠️  Ошибка получения данных: {e}")
    
    print(f"\nВарианты:")
    print(f"  y - подтвердить маппинг")
    print(f"  n - отклонить")
    print(f"  s - пропустить (решить позже)")
    print(f"  id - ввести другой ID")
    
    choice = input("Ваш выбор: ").strip().lower()
    
    if choice == 'y':
        return fund_id
    elif choice == 'n':
        return None
    elif choice == 's':
        return 'skip'
    elif choice.startswith('id'):
        try:
            new_id = int(choice.split()[-1]) if len(choice.split()) > 1 else int(input("Введите ID: "))
            return new_id
        except ValueError:
            print("❌ Неверный формат ID")
            return 'skip'
    else:
        return 'skip'

def main():
    """Основная функция"""
    
    print("🔍 Инструмент ручной проверки фондов")
    print("=" * 50)
    
    # Загружаем кандидатов
    candidates = load_review_candidates()
    
    if candidates.empty:
        print("❌ Нет кандидатов для проверки")
        return
    
    print(f"📋 Найдено {len(candidates)} фондов для проверки")
    
    # Загружаем существующий маппинг
    try:
        with open('auto_fund_mapping.json', 'r', encoding='utf-8') as f:
            mapping_data = json.load(f)
            current_mapping = mapping_data.get('mapping', {})
    except:
        current_mapping = {}
    
    new_mappings = {}
    skipped = []
    
    for idx, row in candidates.iterrows():
        ticker = row['ticker']
        fund_id = row['best_match_id']
        expected_name = row['search_name']
        found_name = row['best_match_name']
        confidence = row['confidence']
        
        print(f"\n{'='*60}")
        print(f"📈 Прогресс: {idx-candidates.index[0]+1}/{len(candidates)}")
        print(f"🎯 Уверенность: {confidence:.2f}")
        
        result = verify_fund_manually(ticker, fund_id, expected_name, found_name)
        
        if result == 'skip':
            skipped.append(ticker)
            print(f"⏭️  Пропущен: {ticker}")
        elif result is None:
            print(f"❌ Отклонен: {ticker}")
        elif isinstance(result, int):
            new_mappings[ticker] = result
            print(f"✅ Добавлен: {ticker} -> {result}")
    
    # Сохраняем обновленный маппинг
    if new_mappings:
        current_mapping.update(new_mappings)
        
        updated_data = {
            'mapping': current_mapping,
            'stats': {
                'total_mapped': len(current_mapping),
                'manually_verified': len(new_mappings),
                'auto_mapped': len(current_mapping) - len(new_mappings)
            },
            'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open('auto_fund_mapping.json', 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Маппинг обновлен!")
        print(f"   ✅ Добавлено: {len(new_mappings)} фондов")
        print(f"   ⏭️  Пропущено: {len(skipped)} фондов")
        print(f"   📊 Всего замаплено: {len(current_mapping)}/96 = {len(current_mapping)/96*100:.1f}%")
        
        # Обновляем investfunds_parser.py
        print(f"\n🔄 Обновляем investfunds_parser.py...")
        update_parser_mapping(current_mapping)
        
    else:
        print(f"\n📝 Новых маппингов не добавлено")
    
    if skipped:
        print(f"\n⏭️  Пропущенные фонды: {', '.join(skipped)}")

def update_parser_mapping(mapping):
    """Обновляет маппинг в investfunds_parser.py"""
    
    parser_file = Path('investfunds_parser.py')
    
    if not parser_file.exists():
        print("❌ Файл investfunds_parser.py не найден")
        return
    
    # Читаем файл
    with open(parser_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Создаем новый маппинг
    mapping_lines = []
    mapping_lines.append("        # Маппинг тикеров на ID фондов (автоматически обновлен)")
    mapping_lines.append("        self.fund_mapping = {")
    
    for ticker, fund_id in sorted(mapping.items()):
        mapping_lines.append(f"            '{ticker}': {fund_id},")
    
    mapping_lines.append("        }")
    
    new_mapping_text = "\n".join(mapping_lines)
    
    # Заменяем старый маппинг
    import re
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
    
    print(f"✅ Файл investfunds_parser.py обновлен с {len(mapping)} фондами")

if __name__ == "__main__":
    main()