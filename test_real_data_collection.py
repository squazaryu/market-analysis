"""
Тест реального сбора данных о российских БПИФах
"""

from etf_data_collector import ETFDataCollectorWithFallback
from logger_config import logger
import json


def test_real_data_collection():
    """Тестирование реального сбора данных"""
    logger.info("=== ТЕСТ РЕАЛЬНОГО СБОРА ДАННЫХ О РОССИЙСКИХ БПИФ ===")
    
    # Создаем коллектор
    collector = ETFDataCollectorWithFallback()
    
    # Проверяем статус провайдеров
    logger.info("Проверка статуса провайдеров:")
    status = collector.get_provider_status()
    for provider in status['providers']:
        logger.info(f"  {provider['name']}: {provider['status']} (приоритет: {provider['priority']})")
    
    # Тестируем сбор данных по одному БПИФу
    logger.info("\nТест сбора данных по SBMX:")
    try:
        sbmx_data = collector.collect_etf_data('SBMX')
        logger.info(f"  Источник: {sbmx_data.get('data_source')}")
        logger.info(f"  Цена: {sbmx_data.get('current_price')} руб")
        logger.info(f"  Доходность: {sbmx_data.get('annual_return')}%")
        logger.info(f"  Качество данных: {sbmx_data.get('data_quality_score'):.2f}")
    except Exception as e:
        logger.error(f"Ошибка сбора данных по SBMX: {e}")
    
    # Тестируем получение списка БПИФов
    logger.info("\nТест получения списка БПИФов:")
    try:
        etf_list = collector.get_etf_list()
        logger.info(f"  Получено БПИФов: {len(etf_list)}")
        for etf in etf_list[:3]:  # Показываем первые 3
            logger.info(f"    {etf.get('ticker')}: {etf.get('name')}")
    except Exception as e:
        logger.error(f"Ошибка получения списка БПИФов: {e}")
    
    # Тестируем получение макроданных
    logger.info("\nТест получения макроэкономических данных:")
    try:
        macro_data = collector.get_macro_data()
        logger.info(f"  Источник: {macro_data.get('data_source')}")
        if 'currency_rates' in macro_data and macro_data['currency_rates']:
            rates = macro_data['currency_rates'].get('rates', {})
            if 'USD' in rates:
                usd_rate = rates['USD']
                logger.info(f"  Курс USD: {usd_rate.get('value')} руб")
    except Exception as e:
        logger.error(f"Ошибка получения макроданных: {e}")
    
    logger.info("\n=== ТЕСТ ЗАВЕРШЕН ===")


if __name__ == "__main__":
    test_real_data_collection()