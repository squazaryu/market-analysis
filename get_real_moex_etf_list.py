"""
Получение реального списка БПИФов с MOEX и их анализ
"""

import requests
import pandas as pd
from datetime import datetime
from logger_config import logger
from etf_data_collector import ETFDataCollectorWithFallback
import time
import json


def get_all_moex_etfs():
    """Получение полного списка БПИФов с MOEX"""
    logger.info("Получение полного списка БПИФов с MOEX...")
    
    try:
        # Запрос к MOEX API для получения всех ETF
        url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQTF/securities.json"
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        etf_list = []
        if 'securities' in data and 'data' in data['securities']:
            columns = data['securities']['columns']
            
            for row in data['securities']['data']:
                security_info = dict(zip(columns, row))
                
                # Фильтруем только ETF
                if security_info.get('SECTYPE') == 'ETF':
                    etf_list.append({
                        'ticker': security_info.get('SECID'),
                        'name': security_info.get('SHORTNAME'),
                        'full_name': security_info.get('SECNAME'),
                        'lot_size': security_info.get('LOTSIZE'),
                        'currency': security_info.get('FACEUNIT', 'RUB'),
                        'list_level': security_info.get('LISTLEVEL'),
                        'trading_status': security_info.get('TRADINGSTATUS')
                    })
        
        logger.info(f"Получено {len(etf_list)} БПИФов с MOEX")
        
        # Показываем список
        logger.info("Список БПИФов с MOEX:")
        for etf in etf_list:
            logger.info(f"  {etf['ticker']}: {etf['name']} (статус: {etf.get('trading_status', 'N/A')})")
        
        return etf_list
        
    except Exception as e:
        logger.error(f"Ошибка получения списка БПИФов с MOEX: {e}")
        return []


def analyze_all_moex_etfs():
    """Анализ всех БПИФов с MOEX"""
    logger.info("=== ПОЛНЫЙ АНАЛИЗ ВСЕХ БПИФ С МОСКОВСКОЙ БИРЖИ ===")
    
    # Получаем реальный список с MOEX
    moex_etfs = get_all_moex_etfs()
    
    if not moex_etfs:
        logger.error("Не удалось получить список БПИФов с MOEX")
        return
    
    # Создаем коллектор
    collector = ETFDataCollectorWithFallback()
    
    # Анализируем каждый БПИФ
    successful_data = []
    failed_tickers = []
    
    logger.info(f"Начинаем детальный анализ {len(moex_etfs)} БПИФов...")
    
    for i, etf_info in enumerate(moex_etfs, 1):
        ticker = etf_info['ticker']
        logger.info(f"[{i}/{len(moex_etfs)}] Анализ {ticker} ({etf_info['name']})...")
        
        try:
            # Получаем детальные данные
            etf_data = collector.collect_etf_data(ticker)
            
            # Добавляем информацию с MOEX
            etf_data.update({
                'moex_name': etf_info['name'],
                'moex_full_name': etf_info['full_name'],
                'lot_size': etf_info['lot_size'],
                'list_level': etf_info['list_level'],
                'trading_status': etf_info['trading_status']
            })
            
            if etf_data.get('current_price') is not None:
                successful_data.append(etf_data)
                price = etf_data.get('current_price')
                return_val = etf_data.get('annual_return')
                logger.info(f"  ✓ {ticker}: цена {price} руб, доходность {return_val}%")
            else:
                failed_tickers.append(ticker)
                logger.warning(f"  ✗ {ticker}: нет ценовых данных")
                
        except Exception as e:
            failed_tickers.append(ticker)
            logger.error(f"  ✗ {ticker}: ошибка - {e}")
        
        # Пауза между запросами
        time.sleep(0.5)
        
        # Промежуточная статистика
        if i % 5 == 0:
            logger.info(f"Промежуточная статистика: успешно {len(successful_data)}, неудачно {len(failed_tickers)}")
    
    # Создаем итоговый отчет
    if successful_data:
        df = pd.DataFrame(successful_data)
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        csv_file = f'complete_moex_etf_analysis_{timestamp}.csv'
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        # Создаем детальный отчет
        report = create_detailed_analysis_report(df, moex_etfs, successful_data, failed_tickers)
        
        json_file = f'complete_moex_etf_report_{timestamp}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # Выводим результаты
        print_comprehensive_results(report, df)
        
        logger.info(f"Полный анализ завершен!")
        logger.info(f"  📊 Данные: {csv_file}")
        logger.info(f"  📋 Отчет: {json_file}")
        
        return df, report
    
    else:
        logger.error("Не удалось получить данные ни по одному БПИФу")
        return None, None


def create_detailed_analysis_report(df, moex_etfs, successful_data, failed_tickers):
    """Создание детального отчета анализа"""
    
    # Анализ доходности
    valid_returns = df[df['annual_return'].notna()]
    performance_analysis = {}
    
    if len(valid_returns) > 0:
        performance_analysis = {
            'total_with_returns': len(valid_returns),
            'average_return': valid_returns['annual_return'].mean(),
            'median_return': valid_returns['annual_return'].median(),
            'min_return': valid_returns['annual_return'].min(),
            'max_return': valid_returns['annual_return'].max(),
            'std_return': valid_returns['annual_return'].std(),
            'positive_returns': (valid_returns['annual_return'] > 0).sum(),
            'negative_returns': (valid_returns['annual_return'] < 0).sum(),
            'top_performers': valid_returns.nlargest(5, 'annual_return')[['ticker', 'moex_name', 'annual_return']].to_dict('records'),
            'worst_performers': valid_returns.nsmallest(5, 'annual_return')[['ticker', 'moex_name', 'annual_return']].to_dict('records')
        }
    
    # Анализ волатильности
    valid_volatility = df[df['volatility'].notna()]
    risk_analysis = {}
    
    if len(valid_volatility) > 0:
        risk_analysis = {
            'total_with_volatility': len(valid_volatility),
            'average_volatility': valid_volatility['volatility'].mean(),
            'median_volatility': valid_volatility['volatility'].median(),
            'min_volatility': valid_volatility['volatility'].min(),
            'max_volatility': valid_volatility['volatility'].max(),
            'low_risk_funds': valid_volatility[valid_volatility['volatility'] < 15][['ticker', 'moex_name', 'volatility']].to_dict('records'),
            'high_risk_funds': valid_volatility[valid_volatility['volatility'] > 30][['ticker', 'moex_name', 'volatility']].to_dict('records')
        }
    
    # Анализ ликвидности
    valid_volume = df[df['daily_volume'].notna()]
    liquidity_analysis = {}
    
    if len(valid_volume) > 0:
        liquidity_analysis = {
            'total_with_volume': len(valid_volume),
            'average_volume': valid_volume['daily_volume'].mean(),
            'median_volume': valid_volume['daily_volume'].median(),
            'most_liquid': valid_volume.nlargest(5, 'daily_volume')[['ticker', 'moex_name', 'daily_volume']].to_dict('records'),
            'least_liquid': valid_volume.nsmallest(5, 'daily_volume')[['ticker', 'moex_name', 'daily_volume']].to_dict('records')
        }
    
    return {
        'analysis_metadata': {
            'analysis_date': datetime.now().isoformat(),
            'total_moex_etfs': len(moex_etfs),
            'successful_analysis': len(successful_data),
            'failed_analysis': len(failed_tickers),
            'success_rate_percent': len(successful_data) / len(moex_etfs) * 100
        },
        'performance_analysis': performance_analysis,
        'risk_analysis': risk_analysis,
        'liquidity_analysis': liquidity_analysis,
        'failed_tickers': failed_tickers,
        'data_quality_summary': {
            'records_with_price': df['current_price'].notna().sum(),
            'records_with_returns': df['annual_return'].notna().sum(),
            'records_with_volatility': df['volatility'].notna().sum(),
            'records_with_volume': df['daily_volume'].notna().sum(),
            'average_quality_score': df['data_quality_score'].mean()
        }
    }


def print_comprehensive_results(report, df):
    """Вывод комплексных результатов"""
    print("\n" + "="*80)
    print("🏆 ПОЛНЫЙ АНАЛИЗ РОССИЙСКИХ БПИФ С МОСКОВСКОЙ БИРЖИ")
    print("="*80)
    
    meta = report['analysis_metadata']
    print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего БПИФов на MOEX: {meta['total_moex_etfs']}")
    print(f"   Успешно проанализировано: {meta['successful_analysis']}")
    print(f"   Процент успешности: {meta['success_rate_percent']:.1f}%")
    
    # Анализ доходности
    if 'performance_analysis' in report and report['performance_analysis']:
        perf = report['performance_analysis']
        print(f"\n💰 АНАЛИЗ ДОХОДНОСТИ:")
        print(f"   Фондов с данными о доходности: {perf['total_with_returns']}")
        print(f"   Средняя доходность: {perf['average_return']:.2f}%")
        print(f"   Диапазон доходности: {perf['min_return']:.2f}% - {perf['max_return']:.2f}%")
        print(f"   Фондов с положительной доходностью: {perf['positive_returns']}")
        
        if perf.get('top_performers'):
            print(f"\n🥇 ТОП-5 ПО ДОХОДНОСТИ:")
            for i, fund in enumerate(perf['top_performers'], 1):
                print(f"   {i}. {fund['ticker']} ({fund['moex_name']}): {fund['annual_return']:.2f}%")
    
    # Анализ рисков
    if 'risk_analysis' in report and report['risk_analysis']:
        risk = report['risk_analysis']
        print(f"\n⚠️ АНАЛИЗ РИСКОВ:")
        print(f"   Фондов с данными о волатильности: {risk['total_with_volatility']}")
        print(f"   Средняя волатильность: {risk['average_volatility']:.2f}%")
        print(f"   Диапазон волатильности: {risk['min_volatility']:.2f}% - {risk['max_volatility']:.2f}%")
        
        if risk.get('low_risk_funds'):
            print(f"\n🛡️ НИЗКОРИСКОВАННЫЕ ФОНДЫ (волатильность < 15%):")
            for fund in risk['low_risk_funds']:
                print(f"   {fund['ticker']} ({fund['moex_name']}): {fund['volatility']:.2f}%")
    
    # Анализ ликвидности
    if 'liquidity_analysis' in report and report['liquidity_analysis']:
        liq = report['liquidity_analysis']
        print(f"\n💧 АНАЛИЗ ЛИКВИДНОСТИ:")
        print(f"   Фондов с данными об объемах: {liq['total_with_volume']}")
        print(f"   Средний дневной объем: {liq['average_volume']:,.0f}")
        
        if liq.get('most_liquid'):
            print(f"\n🌊 САМЫЕ ЛИКВИДНЫЕ ФОНДЫ:")
            for i, fund in enumerate(liq['most_liquid'], 1):
                print(f"   {i}. {fund['ticker']} ({fund['moex_name']}): {fund['daily_volume']:,.0f}")
    
    # Качество данных
    quality = report['data_quality_summary']
    print(f"\n📊 КАЧЕСТВО ДАННЫХ:")
    print(f"   Записей с ценами: {quality['records_with_price']}")
    print(f"   Записей с доходностью: {quality['records_with_returns']}")
    print(f"   Записей с волатильностью: {quality['records_with_volatility']}")
    print(f"   Записей с объемами: {quality['records_with_volume']}")
    print(f"   Средняя оценка качества: {quality['average_quality_score']:.2f}")


if __name__ == "__main__":
    analyze_all_moex_etfs()