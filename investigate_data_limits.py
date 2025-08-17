#!/usr/bin/env python3
"""
Исследование ограничений данных MOEX API и возможностей получения более длинной истории
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time

def test_moex_data_limits():
    """Тестирует, какие данные доступны в MOEX API"""
    
    # Тестовые тикеры популярных БПИФ
    test_tickers = ['TMOS', 'SBGB', 'RUSE', 'VTBG', 'AKMM']
    
    print("🔍 Исследование ограничений данных MOEX API...")
    
    for ticker in test_tickers:
        print(f"\n📊 Тестирование {ticker}:")
        
        # Тест 1: Запрос данных за 2 года
        test_periods = [
            (365, "1 год"),
            (730, "2 года"), 
            (1095, "3 года"),
            (1460, "4 года"),
            (1825, "5 лет")
        ]
        
        for days, description in test_periods:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'till': end_date.strftime('%Y-%m-%d'),
                'interval': 24
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'candles' in data and 'data' in data['candles']:
                        candles_data = data['candles']['data']
                        data_points = len(candles_data)
                        
                        if data_points > 0:
                            # Получаем первую и последнюю дату
                            columns = data['candles']['columns']
                            df = pd.DataFrame(candles_data, columns=columns)
                            
                            if 'begin' in df.columns:
                                df['begin'] = pd.to_datetime(df['begin'])
                                first_date = df['begin'].min()
                                last_date = df['begin'].max()
                                actual_days = (last_date - first_date).days
                                
                                print(f"   ✅ {description}: {data_points} точек, с {first_date.strftime('%Y-%m-%d')} по {last_date.strftime('%Y-%m-%d')} ({actual_days} дней)")
                            else:
                                print(f"   ✅ {description}: {data_points} точек данных")
                        else:
                            print(f"   ❌ {description}: Нет данных")
                    else:
                        print(f"   ❌ {description}: Некорректный ответ API")
                else:
                    print(f"   ❌ {description}: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ {description}: Ошибка - {e}")
            
            time.sleep(0.5)  # Не перегружаем API
    
    print("\n" + "="*60)
    
    # Тест 2: Проверяем, есть ли данные за конкретные исторические периоды
    historical_periods = [
        ("2020-02-01", "2020-06-01", "COVID кризис"),
        ("2022-02-24", "2022-12-31", "Санкции 2022"), 
        ("2019-01-01", "2019-12-31", "2019 год"),
        ("2021-01-01", "2021-12-31", "2021 год")
    ]
    
    print("\n🕰️ Проверка доступности исторических периодов:")
    
    for start_str, end_str, description in historical_periods:
        print(f"\n📅 {description} ({start_str} - {end_str}):")
        
        available_tickers = []
        
        for ticker in test_tickers[:3]:  # Проверяем только первые 3 для экономии времени
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
            params = {
                'from': start_str,
                'till': end_str,
                'interval': 24
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'candles' in data and 'data' in data['candles']:
                        data_points = len(data['candles']['data'])
                        
                        if data_points > 0:
                            available_tickers.append(f"{ticker}({data_points} точек)")
                            print(f"   ✅ {ticker}: {data_points} точек данных")
                        else:
                            print(f"   ❌ {ticker}: Нет данных")
                    else:
                        print(f"   ❌ {ticker}: Некорректный ответ")
                        
            except Exception as e:
                print(f"   ❌ {ticker}: Ошибка - {e}")
            
            time.sleep(0.3)
        
        if available_tickers:
            print(f"   📊 Доступны данные: {', '.join(available_tickers)}")
        else:
            print(f"   ⚠️  Нет доступных данных за этот период")

def check_fund_inception_dates():
    """Проверяет даты создания фондов"""
    
    print("\n📈 Проверка дат создания фондов:")
    
    # Загружаем актуальные данные о фондах
    try:
        from pathlib import Path
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
        
        if data_files:
            latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
            df = pd.read_csv(latest_data)
            
            if 'inception_date' in df.columns:
                df['inception_date'] = pd.to_datetime(df['inception_date'], errors='coerce')
                df_valid = df.dropna(subset=['inception_date'])
                
                if len(df_valid) > 0:
                    print(f"   📊 Фондов с известной датой создания: {len(df_valid)}")
                    print(f"   📅 Самый старый фонд: {df_valid['inception_date'].min().strftime('%Y-%m-%d')}")
                    print(f"   📅 Самый новый фонд: {df_valid['inception_date'].max().strftime('%Y-%m-%d')}")
                    
                    # Фонды, созданные до COVID
                    pre_covid = df_valid[df_valid['inception_date'] < '2020-02-01']
                    print(f"   🦠 Фондов до COVID (до 2020-02-01): {len(pre_covid)}")
                    
                    if len(pre_covid) > 0:
                        print("   📋 Старейшие фонды:")
                        for _, row in pre_covid.nsmallest(10, 'inception_date').iterrows():
                            print(f"      {row['ticker']}: {row['inception_date'].strftime('%Y-%m-%d')}")
                else:
                    print("   ❌ Нет валидных дат создания")
            else:
                print("   ❌ Колонка inception_date не найдена")
        else:
            print("   ❌ Файлы данных не найдены")
            
    except Exception as e:
        print(f"   ❌ Ошибка анализа дат: {e}")

def suggest_data_expansion_strategy():
    """Предлагает стратегию расширения данных"""
    
    print("\n" + "="*60)
    print("💡 РЕКОМЕНДАЦИИ ПО РАСШИРЕНИЮ ДАННЫХ:")
    print("="*60)
    
    print("""
1. 📈 РАСШИРЕНИЕ ИСТОРИЧЕСКИХ ДАННЫХ:
   • MOEX API поддерживает запросы до 5+ лет истории
   • Нужно изменить параметр days в get_historical_data()
   • Создать кэш для длинных исторических периодов
   
2. 🔄 СТРАТЕГИЯ ДЛЯ АНАЛИЗА COVID/КРИЗИСОВ:
   • Запрашивать данные за конкретные даты (не относительно сегодня)
   • Использовать только фонды, существовавшие в тот период
   • Кэшировать исторические периоды отдельно
   
3. 📊 АНАЛИЗ СЧА И ПРИТОКОВ:
   • Добавить запрос market_cap из MOEX API
   • Отслеживать изменения объемов торгов
   • Анализировать относительные изменения размеров фондов
   
4. ⚡ АВТООБНОВЛЕНИЕ:
   • Cron job или планировщик для еженедельного обновления
   • Инкрементальное обновление новых данных
   • Сохранение исторических снимков данных
   
5. 🎯 УЛУЧШЕНИЯ ВРЕМЕННОГО АНАЛИЗА:
   • Предупреждения о недостатке данных для периода
   • Альтернативные метрики для коротких периодов
   • Использование benchmark данных (индексы MOEX)
""")

if __name__ == "__main__":
    test_moex_data_limits()
    check_fund_inception_dates()
    suggest_data_expansion_strategy()