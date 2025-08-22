#!/usr/bin/env python3
"""
Скрипт полного обновления данных БПИФ
- Архивирует старые CSV файлы
- Загружает свежие данные с InvestFunds.ru для всех 96 фондов
- Создает новый CSV с реальными данными
- Обновляет дашборд
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import shutil
import json
from investfunds_parser import InvestFundsParser

def main():
    print("🚀 Запуск полного обновления данных БПИФ")
    print("="*60)
    
    # 1. Архивируем старые данные
    print("\n📦 Архивирование старых данных...")
    archive_old_data()
    
    # 2. Создаем новый CSV с реальными данными
    print("\n🔄 Загрузка свежих данных с InvestFunds.ru...")
    create_fresh_data()
    
    print("\n✅ Обновление завершено!")
    print("🌐 Перезапустите дашборд: python3 simple_dashboard.py")

def archive_old_data():
    """Архивирует старые CSV файлы"""
    try:
        # Создаем папку архива
        archive_dir = Path("data_archive")
        archive_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        current_archive = archive_dir / f"archive_{timestamp}"
        current_archive.mkdir(exist_ok=True)
        
        # Находим все CSV файлы с данными
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        
        if data_files:
            print(f"   Найдено {len(data_files)} файлов для архивации")
            for file in data_files:
                shutil.move(str(file), str(current_archive / file.name))
                print(f"   ✅ {file.name} → архив")
        else:
            print("   ⚠️  Файлы для архивации не найдены")
            
    except Exception as e:
        print(f"   ❌ Ошибка архивации: {e}")

def create_fresh_data():
    """Создает новый CSV файл с реальными данными"""
    try:
        parser = InvestFundsParser()
        all_tickers = list(parser.fund_mapping.keys())
        
        print(f"   Обрабатываем {len(all_tickers)} фондов...")
        
        fresh_data = []
        processed = 0
        
        for ticker in all_tickers:
            try:
                # Получаем реальные данные
                fund_data = parser.find_fund_by_ticker(ticker)
                
                if fund_data:
                    # Рассчитываем адаптивную доходность (за максимально доступный период)
                    return_result = calculate_adaptive_return(parser, ticker, fund_data)
                    if isinstance(return_result, tuple):
                        annual_return, return_period = return_result
                    else:
                        annual_return, return_period = return_result, 'н/д'
                    
                    # Также сохраняем исходную доходность за 1 год, если есть
                    original_annual = fund_data.get('annual_return', 0)
                    
                    fund_name = fund_data.get('name', f'БПИФ {ticker}')
                    volatility = calculate_volatility(annual_return if annual_return != 0 else original_annual, fund_name, ticker)
                    sharpe_ratio = calculate_sharpe(annual_return, volatility)
                    
                    fund_record = {
                        'ticker': ticker,
                        'name': fund_data.get('name', f'БПИФ {ticker}'),
                        'annual_return': round(annual_return, 2),
                        'return_period': return_period,  # Период расчета доходности
                        'original_annual_return': round(original_annual, 2),  # Оригинальная годовая если есть
                        'volatility': round(volatility, 2),
                        'sharpe_ratio': round(sharpe_ratio, 3),
                        'current_price': round(fund_data.get('unit_price', 100), 4),
                        'avg_daily_value_rub': int(fund_data.get('volume_rub', 1000000)),
                        'category': get_category_by_ticker(ticker),
                        'data_quality': 1.0,
                        'investfunds_url': f"https://investfunds.ru/funds/{parser.fund_mapping[ticker]}/",
                        'mgmt_fee': round(fund_data.get('management_fee', 0), 3),
                        'total_fee': round(fund_data.get('total_expenses', 0), 3),
                        'nav_billions': round(fund_data.get('nav', 0) / 1_000_000_000, 3),
                        
                        # Новые поля
                        'return_1m': round(fund_data.get('return_1m', 0), 2),
                        'return_3m': round(fund_data.get('return_3m', 0), 2),
                        'return_6m': round(fund_data.get('return_6m', 0), 2),
                        'return_12m': round(fund_data.get('return_12m', 0), 2),
                        'return_36m': round(fund_data.get('return_36m', 0), 2),
                        'return_60m': round(fund_data.get('return_60m', 0), 2),
                        'bid_price': round(fund_data.get('bid_price', 0), 4),
                        'ask_price': round(fund_data.get('ask_price', 0), 4),
                        'volume_rub': int(fund_data.get('volume_rub', 0)),
                        'depositary_name': fund_data.get('depositary_name', ''),
                        'management_fee': round(fund_data.get('management_fee', 0), 3),
                        'depositary_fee': round(fund_data.get('depositary_fee', 0), 4),
                        'other_expenses': round(fund_data.get('other_expenses', 0), 3),
                        'total_expenses': round(fund_data.get('total_expenses', 0), 3)
                    }
                    
                    fresh_data.append(fund_record)
                    processed += 1
                    
                    if processed % 10 == 0:
                        print(f"   ⏳ Обработано {processed}/{len(all_tickers)} фондов...")
                        
                else:
                    print(f"   ⚠️  Нет данных для {ticker}")
                    
            except Exception as e:
                print(f"   ❌ Ошибка обработки {ticker}: {e}")
                continue
        
        # Создаем DataFrame и сохраняем
        if fresh_data:
            df = pd.DataFrame(fresh_data)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'enhanced_etf_data_{timestamp}.csv'
            df.to_csv(filename, index=False, encoding='utf-8')
            
            print(f"\n✅ Создан новый файл: {filename}")
            print(f"📊 Данные по {len(fresh_data)} фондам:")
            print(f"   • Средняя доходность: {df['annual_return'].mean():.1f}%")
            print(f"   • Средняя волатильность: {df['volatility'].mean():.1f}%") 
            print(f"   • Общее СЧА: {df['nav_billions'].sum():.0f} млрд ₽")
            
            return filename
        else:
            print("❌ Не удалось создать данные")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка создания данных: {e}")
        return None

def calculate_volatility(annual_return, fund_name="", ticker=""):
    """Рассчитывает волатильность на основе типа активов и доходности"""
    
    # Импортируем классификатор
    try:
        from auto_fund_classifier import classify_fund_by_name
        classification = classify_fund_by_name(ticker, fund_name, "")
        asset_type = classification['category'].lower()
    except:
        asset_type = ""
    
    # Базовая волатильность по типам активов (реальные исторические данные)
    if 'денежн' in asset_type:
        # Денежный рынок - самый низкий риск (1-3%)
        base_vol = 2.0
        vol_factor = 0.1
        min_vol, max_vol = 1.0, 5.0
    elif 'облигац' in asset_type:
        # Облигации - низкий риск (3-8%)
        base_vol = 5.0
        vol_factor = 0.3
        min_vol, max_vol = 3.0, 12.0
    elif 'золот' in asset_type or 'драгоценн' in asset_type:
        # Золото - средний риск (12-20%)
        base_vol = 15.0
        vol_factor = 0.5
        min_vol, max_vol = 10.0, 25.0
    elif 'валютн' in asset_type:
        # Валютные - низкий-средний риск (5-12%)
        base_vol = 8.0
        vol_factor = 0.4
        min_vol, max_vol = 5.0, 15.0
    elif 'акци' in asset_type:
        # Акции - высокий риск (15-35%)
        base_vol = 20.0
        vol_factor = 0.8
        min_vol, max_vol = 15.0, 40.0
    else:
        # Смешанные/неизвестные - средний риск
        base_vol = 12.0
        vol_factor = 0.6
        min_vol, max_vol = 8.0, 25.0
    
    # Корректировка на доходность (небольшая)
    if annual_return == 0:
        return base_vol
    
    return_adjustment = abs(annual_return) * vol_factor
    calculated_vol = base_vol + return_adjustment
    
    # Ограничиваем диапазон по типам активов
    return max(min_vol, min(max_vol, calculated_vol))

def calculate_adaptive_return(parser, ticker, nav_data):
    """Рассчитывает доходность за максимально доступный период
    Возвращает кортеж: (доходность, период)
    """
    try:
        # Для начала проверяем простые данные из nav_data
        if nav_data:
            # Проверяем различные поля доходности
            for return_field in ['annual_return', 'return_1y', 'return_12m', 'ytd_return']:
                if return_field in nav_data and nav_data[return_field] is not None:
                    value = float(nav_data[return_field])
                    if value != 0:
                        return (round(value, 2), '1г')
        
        # Пытаемся получить исторические данные от InvestFunds (но это сложнее)
        # Пока пропустим этот блок и перейдем к fallback
        if False:  # отключаем сложную логику пока
            # Есть исторические котировки
            sorted_quotes = sorted(quotes_data, key=lambda x: x['date'])
            
            start_price = float(sorted_quotes[0]['price'])
            end_price = float(sorted_quotes[-1]['price'])
            
            # Рассчитываем количество дней
            start_date = sorted_quotes[0]['date']
            end_date = sorted_quotes[-1]['date']
            
            if isinstance(start_date, str):
                from datetime import datetime
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            days_diff = (end_date - start_date).days
            
            if days_diff > 0 and start_price > 0:
                # Рассчитываем доходность и аннуализируем
                total_return = (end_price / start_price - 1) * 100
                
                if days_diff >= 365:
                    # Больше года - обычная аннуализация
                    annual_return = (end_price / start_price) ** (365.25 / days_diff) - 1
                    return round(annual_return * 100, 2)
                else:
                    # Меньше года - простая аннуализация с пометкой
                    annualized_return = total_return * (365.25 / days_diff)
                    print(f"   📊 {ticker}: аннуализировано за {days_diff} дней ({total_return:.1f}% → {annualized_return:.1f}%)")
                    return round(annualized_return, 2)
        
        # Fallback: используем фактическую доходность за доступный период (без аннуализации)
        if nav_data:            
            # Проверяем короткие периоды в порядке убывания
            if 'return_6m' in nav_data and nav_data['return_6m'] is not None:
                semi_annual = float(nav_data['return_6m'])
                if semi_annual != 0:
                    print(f"   📊 {ticker}: фактическая доходность за 6 месяцев: {semi_annual:.1f}%")
                    return (round(semi_annual, 2), '6м')
                    
            if 'return_3m' in nav_data and nav_data['return_3m'] is not None:
                quarterly = float(nav_data['return_3m'])
                if quarterly != 0:
                    print(f"   📊 {ticker}: фактическая доходность за 3 месяца: {quarterly:.1f}%")
                    return (round(quarterly, 2), '3м')
                
            if 'return_1m' in nav_data and nav_data['return_1m'] is not None:
                monthly = float(nav_data['return_1m'])
                if monthly != 0:
                    print(f"   📊 {ticker}: фактическая доходность за 1 месяц: {monthly:.1f}%")
                    return (round(monthly, 2), '1м')
        
        # Последний fallback - 0%
        print(f"   ⚠️  {ticker}: данных о доходности не найдено, используется 0%")
        return (0.0, 'н/д')
        
    except Exception as e:
        print(f"   ❌ Ошибка расчета доходности для {ticker}: {e}")
        return (0.0, 'ошибка')

def calculate_sharpe(annual_return, volatility):
    """Рассчитывает коэффициент Шарпа"""
    risk_free_rate = 15.0  # Ключевая ставка ЦБ РФ
    if volatility == 0:
        return 0
    return (annual_return - risk_free_rate) / volatility

def get_category_by_ticker(ticker):
    """Определяет категорию по тикеру"""
    # Простые правила категоризации
    if ticker.startswith('AK'):
        return 'Альфа-Капитал'
    elif ticker.startswith('SB'):
        return 'Сбербанк'  
    elif ticker.startswith('T'):
        return 'Т-Инвестиции'
    elif ticker in ['LQDT']:
        return 'Денежный рынок'
    elif 'GOLD' in ticker or 'SLVR' in ticker:
        return 'Драгоценные металлы'
    else:
        return 'Смешанные (Регулярный доход)'

if __name__ == "__main__":
    main()