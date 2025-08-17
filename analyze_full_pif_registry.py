"""
Анализ полного реестра БПИФов из файла ЦБ РФ
"""

import pandas as pd
import numpy as np
from datetime import datetime
from logger_config import logger
from etf_data_collector import ETFDataCollectorWithFallback
import time
import json


def load_cbr_pif_registry():
    """Загрузка полного реестра БПИФов из файла ЦБ"""
    logger.info("Загрузка реестра БПИФов из файла ЦБ РФ...")
    
    try:
        # Пробуем разные варианты чтения Excel файла
        file_path = "list_PIF.xlsx"
        
        # Сначала посмотрим на структуру файла
        xl_file = pd.ExcelFile(file_path)
        logger.info(f"Листы в файле: {xl_file.sheet_names}")
        
        # Читаем первый лист
        df = pd.read_excel(file_path, sheet_name=0)
        logger.info(f"Загружено записей: {len(df)}")
        logger.info(f"Колонки: {list(df.columns)}")
        
        # Показываем первые несколько записей для понимания структуры
        logger.info("Первые 5 записей:")
        for i, row in df.head().iterrows():
            logger.info(f"  {i}: {dict(row)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        return None


def extract_etf_tickers_from_registry(df):
    """Извлечение тикеров БПИФов из реестра"""
    logger.info("Извлечение тикеров БПИФов из реестра...")
    
    if df is None:
        return []
    
    # Ищем колонки, которые могут содержать тикеры или коды
    potential_ticker_columns = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['код', 'тикер', 'ticker', 'symbol', 'secid', 'isin']):
            potential_ticker_columns.append(col)
    
    logger.info(f"Потенциальные колонки с тикерами: {potential_ticker_columns}")
    
    # Ищем колонки с типом фонда
    type_columns = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(keyword in col_lower for keyword in ['тип', 'type', 'категория', 'вид']):
            type_columns.append(col)
    
    logger.info(f"Колонки с типом фонда: {type_columns}")
    
    # Фильтруем только БПИФы (биржевые ПИФы)
    etf_df = df.copy()
    
    # Если есть колонка с типом, фильтруем по БПИФам
    if type_columns:
        type_col = type_columns[0]
        logger.info(f"Уникальные типы фондов в колонке '{type_col}':")
        unique_types = df[type_col].value_counts()
        for type_name, count in unique_types.items():
            logger.info(f"  {type_name}: {count}")
        
        # Фильтруем БПИФы
        etf_keywords = ['биржевой', 'бпиф', 'etf', 'exchange', 'traded']
        etf_mask = df[type_col].astype(str).str.lower().str.contains('|'.join(etf_keywords), na=False)
        etf_df = df[etf_mask]
        logger.info(f"Найдено БПИФов после фильтрации: {len(etf_df)}")
    
    # Извлекаем тикеры
    tickers = []
    if potential_ticker_columns:
        ticker_col = potential_ticker_columns[0]
        tickers = etf_df[ticker_col].dropna().astype(str).tolist()
        # Очищаем тикеры от лишних символов
        tickers = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
        # Убираем дубликаты
        tickers = list(set(tickers))
    
    logger.info(f"Извлечено уникальных тикеров: {len(tickers)}")
    logger.info(f"Примеры тикеров: {tickers[:10]}")
    
    return tickers, etf_df


def get_moex_etf_list():
    """Получение списка БПИФов напрямую с MOEX"""
    logger.info("Получение списка БПИФов с MOEX...")
    
    try:
        collector = ETFDataCollectorWithFallback()
        etf_list = collector.get_etf_list()
        
        # Извлекаем тикеры из списка
        moex_tickers = []
        if etf_list:
            for etf in etf_list:
                if etf.get('ticker'):
                    moex_tickers.append(etf['ticker'])
        
        logger.info(f"Получено тикеров с MOEX: {len(moex_tickers)}")
        logger.info(f"MOEX тикеры: {moex_tickers}")
        
        return moex_tickers
        
    except Exception as e:
        logger.error(f"Ошибка получения списка с MOEX: {e}")
        return []


def get_all_russian_etf_tickers():
    """Получение полного списка российских БПИФов из всех источников"""
    logger.info("Сбор полного списка российских БПИФов...")
    
    all_tickers = set()
    
    # 1. Загружаем из реестра ЦБ
    cbr_df = load_cbr_pif_registry()
    if cbr_df is not None:
        cbr_tickers, etf_df = extract_etf_tickers_from_registry(cbr_df)
        all_tickers.update(cbr_tickers)
        logger.info(f"Из реестра ЦБ добавлено: {len(cbr_tickers)} тикеров")
    
    # 2. Получаем с MOEX
    moex_tickers = get_moex_etf_list()
    all_tickers.update(moex_tickers)
    logger.info(f"С MOEX добавлено: {len(moex_tickers)} тикеров")
    
    # 3. Добавляем известные тикеры из нашей базы
    known_tickers = [
        'SBMX', 'VTBX', 'TECH', 'TGLD', 'FXRU', 'FXUS', 'FXGD', 'DIVD', 'SBGB', 'SBCB',
        'FXRB', 'FXRL', 'FXMM', 'FXTP', 'FXCN', 'FXDE', 'FXIT', 'FXKZ', 'FXWO', 'FXRW',
        'TMOS', 'TIPO', 'TBIO', 'TEUR', 'TUSD', 'TCNY', 'TSPX', 'TNPG', 'TRUR', 'TGRU',
        'SBSP', 'SBMM', 'SBGD', 'SBCL', 'SBAG', 'SBPD', 'SBCS', 'SBHI', 'SBEM', 'SBEU',
        'VTBA', 'VTBB', 'VTBE', 'VTBG', 'VTBM', 'VTBN', 'VTBR', 'VTBS', 'VTBU', 'VTBZ',
        'AKSP', 'AKMM', 'AKGD', 'AKCL', 'AKAG', 'AKPD', 'AKCS', 'AKHI', 'AKEM', 'AKEU'
    ]
    all_tickers.update(known_tickers)
    logger.info(f"Из базы знаний добавлено: {len(known_tickers)} тикеров")
    
    # Конвертируем в отсортированный список
    final_tickers = sorted(list(all_tickers))
    
    logger.info(f"ИТОГО уникальных тикеров для анализа: {len(final_tickers)}")
    
    return final_tickers


def analyze_all_russian_etfs():
    """Анализ всех российских БПИФов"""
    logger.info("=== ЗАПУСК ПОЛНОГО АНАЛИЗА ВСЕХ РОССИЙСКИХ БПИФ ===")
    
    # Получаем полный список тикеров
    all_tickers = get_all_russian_etf_tickers()
    
    if not all_tickers:
        logger.error("Не удалось получить список тикеров для анализа")
        return
    
    logger.info(f"Начинаем анализ {len(all_tickers)} БПИФов...")
    
    # Создаем коллектор
    collector = ETFDataCollectorWithFallback()
    
    # Результаты анализа
    successful_data = []
    failed_tickers = []
    
    # Анализируем каждый тикер
    for i, ticker in enumerate(all_tickers, 1):
        logger.info(f"[{i}/{len(all_tickers)}] Анализ {ticker}...")
        
        try:
            etf_data = collector.collect_etf_data(ticker)
            
            if etf_data and etf_data.get('current_price') is not None:
                successful_data.append(etf_data)
                logger.info(f"  ✓ {ticker}: цена {etf_data.get('current_price')} руб, источник: {etf_data.get('data_source')}")
            else:
                failed_tickers.append(ticker)
                logger.warning(f"  ✗ {ticker}: данные недоступны")
                
        except Exception as e:
            failed_tickers.append(ticker)
            logger.error(f"  ✗ {ticker}: ошибка - {e}")
        
        # Пауза между запросами
        time.sleep(0.3)
        
        # Промежуточная статистика каждые 10 тикеров
        if i % 10 == 0:
            logger.info(f"Промежуточная статистика: успешно {len(successful_data)}, неудачно {len(failed_tickers)}")
    
    # Создаем DataFrame с результатами
    if successful_data:
        df = pd.DataFrame(successful_data)
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # CSV файл
        csv_file = f'full_russian_etf_analysis_{timestamp}.csv'
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        # JSON отчет
        report = {
            'analysis_date': datetime.now().isoformat(),
            'total_tickers_analyzed': len(all_tickers),
            'successful_analysis': len(successful_data),
            'failed_analysis': len(failed_tickers),
            'success_rate': len(successful_data) / len(all_tickers) * 100,
            'failed_tickers': failed_tickers,
            'summary_statistics': {
                'total_etfs': len(df),
                'average_price': df['current_price'].mean() if 'current_price' in df.columns else None,
                'price_range': {
                    'min': df['current_price'].min() if 'current_price' in df.columns else None,
                    'max': df['current_price'].max() if 'current_price' in df.columns else None
                },
                'data_sources': df['data_source'].value_counts().to_dict() if 'data_source' in df.columns else {},
                'management_companies': df['management_company'].value_counts().to_dict() if 'management_company' in df.columns else {}
            }
        }
        
        json_file = f'full_russian_etf_report_{timestamp}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # Выводим итоговую статистику
        print_final_statistics(report, df)
        
        logger.info(f"Анализ завершен!")
        logger.info(f"  📊 Данные: {csv_file}")
        logger.info(f"  📋 Отчет: {json_file}")
        
        return df, report
    
    else:
        logger.error("Не удалось получить данные ни по одному БПИФу")
        return None, None


def print_final_statistics(report, df):
    """Вывод итоговой статистики"""
    print("\n" + "="*80)
    print("🏆 ИТОГОВАЯ СТАТИСТИКА ПОЛНОГО АНАЛИЗА РОССИЙСКИХ БПИФ")
    print("="*80)
    
    print(f"\n📊 ОБЩАЯ ИНФОРМАЦИЯ:")
    print(f"   Всего тикеров проанализировано: {report['total_tickers_analyzed']}")
    print(f"   Успешно получены данные: {report['successful_analysis']}")
    print(f"   Неудачных попыток: {report['failed_analysis']}")
    print(f"   Процент успешности: {report['success_rate']:.1f}%")
    
    if 'summary_statistics' in report and report['summary_statistics']:
        stats = report['summary_statistics']
        
        if stats.get('average_price'):
            print(f"\n💰 ЦЕНОВАЯ СТАТИСТИКА:")
            print(f"   Средняя цена: {stats['average_price']:.2f} руб")
            print(f"   Диапазон цен: {stats['price_range']['min']:.2f} - {stats['price_range']['max']:.2f} руб")
        
        if stats.get('data_sources'):
            print(f"\n🔌 ИСТОЧНИКИ ДАННЫХ:")
            for source, count in stats['data_sources'].items():
                print(f"   {source}: {count} фондов")
        
        if stats.get('management_companies'):
            print(f"\n🏢 УПРАВЛЯЮЩИЕ КОМПАНИИ (топ-10):")
            sorted_companies = sorted(stats['management_companies'].items(), key=lambda x: x[1], reverse=True)
            for company, count in sorted_companies[:10]:
                print(f"   {company}: {count} фондов")
    
    if report.get('failed_tickers'):
        print(f"\n❌ НЕУДАЧНЫЕ ТИКЕРЫ (первые 20):")
        for ticker in report['failed_tickers'][:20]:
            print(f"   {ticker}")
        if len(report['failed_tickers']) > 20:
            print(f"   ... и еще {len(report['failed_tickers']) - 20}")


if __name__ == "__main__":
    analyze_all_russian_etfs()