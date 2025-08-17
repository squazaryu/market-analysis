#!/usr/bin/env python3
"""
Генератор списка тикеров без данных с investfunds.ru
"""

import pandas as pd
from pathlib import Path

def generate_missing_tickers_list():
    # Загружаем данные ETF
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if not data_files:
        data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))

    latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
    etf_data = pd.read_csv(latest_data)

    # Известные маппинги (есть данные)
    known_tickers = {'LQDT', 'AKMB', 'AMGL', 'AMNR', 'AMNY', 'AMRE'}

    # Все остальные тикеры (нет данных)
    missing_data = etf_data[~etf_data['ticker'].isin(known_tickers)].copy()

    print('🎯 ПОЛНЫЙ СПИСОК ТИКЕРОВ БЕЗ ДАННЫХ С INVESTFUNDS.RU')
    print('═' * 80)
    print(f'📊 Всего фондов: {len(etf_data)}')
    print(f'✅ Есть данные: {len(known_tickers)} фондов')
    print(f'❌ Нет данных: {len(missing_data)} фондов')
    print()

    # Сортируем по ликвидности если есть данные
    if 'avg_daily_value_rub' in missing_data.columns:
        missing_data = missing_data.sort_values('avg_daily_value_rub', ascending=False, na_last=True)
        
        print('📈 ТОП-20 ПРИОРИТЕТНЫХ ФОНДОВ (по ликвидности):')
        print('═' * 80)
        
        for i, (_, row) in enumerate(missing_data.head(20).iterrows(), 1):
            ticker = row['ticker']
            uc = str(row.get('management_company', 'Unknown'))[:30]
            volume = row.get('avg_daily_value_rub', 0)
            
            if pd.notna(volume) and volume > 0:
                print(f'{i:2d}. {ticker:6} | {uc:30} | {volume/1e6:8.1f} млн ₽/день')
            else:
                print(f'{i:2d}. {ticker:6} | {uc:30} | {"—":>12}')
        print()

    # Группировка по УК
    print('🏢 ГРУППИРОВКА ПО УПРАВЛЯЮЩИМ КОМПАНИЯМ:')
    print('═' * 80)
    
    for uc, group in missing_data.groupby('management_company', dropna=False):
        if pd.isna(uc):
            uc = 'Unknown'
        
        tickers = sorted(group['ticker'].tolist())
        print(f'📍 {uc} ({len(tickers)} фондов):')
        
        # Выводим по 8 тикеров в строке для компактности
        for i in range(0, len(tickers), 8):
            line_tickers = tickers[i:i+8]
            print(f'   {" ".join(t.ljust(6) for t in line_tickers)}')
        print()

    # Полный алфавитный список
    print('📋 ПОЛНЫЙ АЛФАВИТНЫЙ СПИСОК (90 фондов):')
    print('═' * 80)
    
    all_missing = sorted(missing_data['ticker'].tolist())
    
    # Выводим по 10 в строке
    for i in range(0, len(all_missing), 10):
        line_tickers = all_missing[i:i+10]
        line_nums = [f'{j+1:2d}.' for j in range(i, min(i+10, len(all_missing)))]
        
        for num, ticker in zip(line_nums, line_tickers):
            print(f'{num} {ticker:6}', end='  ')
        print()
    
    print()
    print('💾 СПИСОК ДЛЯ КОПИРОВАНИЯ (через запятую):')
    print('═' * 80)
    print(', '.join(all_missing))
    
    print()
    print('💡 ФОРМАТ ДЛЯ ПРЕДОСТАВЛЕНИЯ ССЫЛОК:')
    print('═' * 80)
    print('Пожалуйста, предоставьте ссылки в формате:')
    print('TICKER: https://investfunds.ru/funds/ID/')
    print()
    print('Например:')
    print('AKAI: https://investfunds.ru/funds/12345/')
    print('AKBC: https://investfunds.ru/funds/67890/')
    print('...')

if __name__ == "__main__":
    generate_missing_tickers_list()