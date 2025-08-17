"""
Simple Fallback Manager - базовый менеджер для переключения между источниками данных
"""

import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from fallback_system import DataProvider, FallbackResult, DataSourceStatus, AllProvidersUnavailableError
from moex_provider import MOEXDataProvider
from yahoo_finance_provider import YahooFinanceProvider
from cbr_provider import CBRDataProvider
from logger_config import logger


class DataProviderManager:
    """
    Простой менеджер для управления fallback между провайдерами данных
    Приоритет: MOEX -> Yahoo Finance -> CBR (для макроданных)
    """
    
    def __init__(self):
        # Инициализация провайдеров в порядке приоритета
        self.providers = [
            MOEXDataProvider(),
            YahooFinanceProvider(),
            CBRDataProvider()
        ]
        
        # Сортируем по приоритету (меньше = выше приоритет)
        self.providers.sort(key=lambda p: p.priority)
        
        logger.info(f"Fallback Manager инициализирован с {len(self.providers)} провайдерами")
        for provider in self.providers:
            logger.info(f"  - {provider.name} (приоритет: {provider.priority})")
    
    def get_etf_data_with_fallback(self, ticker: str) -> FallbackResult:
        """
        Получение данных о БПИФе с fallback между провайдерами
        
        Args:
            ticker: Тикер БПИФа (например, SBMX, VTBX)
            
        Returns:
            FallbackResult: Результат с данными и метаинформацией
        """
        errors = {}
        
        for fallback_level, provider in enumerate(self.providers):
            if not provider.enabled:
                logger.debug(f"Провайдер {provider.name} отключен, пропускаем")
                continue
            
            try:
                logger.debug(f"Попытка получить данные для {ticker} из {provider.name}")
                
                # Проверяем здоровье провайдера
                health_status = provider.health_check()
                if health_status == DataSourceStatus.UNAVAILABLE:
                    logger.warning(f"Провайдер {provider.name} недоступен")
                    errors[provider.name] = Exception(f"Provider {provider.name} is unavailable")
                    continue
                
                # Получаем данные в зависимости от типа провайдера
                if provider.name == 'cbr':
                    # CBR не поддерживает данные по тикерам, пропускаем для ETF
                    logger.debug(f"CBR не поддерживает данные по тикерам, пропускаем для {ticker}")
                    continue
                
                # Собираем комплексные данные о БПИФе
                etf_data = self._collect_comprehensive_etf_data(provider, ticker)
                
                if etf_data:
                    # Оценка качества данных
                    quality_score = self._assess_data_quality(etf_data)
                    
                    result = FallbackResult(
                        data=etf_data,
                        source=provider.name,
                        quality_score=quality_score,
                        is_cached=False,  # Пока не реализовано кэширование
                        fallback_level=fallback_level,
                        warnings=[],
                        metadata={
                            'provider_priority': provider.priority,
                            'health_status': health_status.value,
                            'data_timestamp': datetime.now().isoformat()
                        }
                    )
                    
                    logger.info(f"Данные для {ticker} успешно получены из {provider.name} (качество: {quality_score:.2f})")
                    return result
                else:
                    logger.warning(f"Провайдер {provider.name} вернул пустые данные для {ticker}")
                    errors[provider.name] = Exception(f"Empty data returned for {ticker}")
                    
            except Exception as e:
                logger.error(f"Ошибка получения данных из {provider.name} для {ticker}: {e}")
                errors[provider.name] = e
                continue
        
        # Все провайдеры не смогли предоставить данные
        logger.error(f"Все провайдеры не смогли предоставить данные для {ticker}")
        raise AllProvidersUnavailableError(errors)
    
    def get_etf_list_with_fallback(self) -> FallbackResult:
        """
        Получение списка БПИФов с fallback
        
        Returns:
            FallbackResult: Список доступных БПИФов
        """
        errors = {}
        
        for fallback_level, provider in enumerate(self.providers):
            if not provider.enabled:
                continue
            
            try:
                logger.debug(f"Попытка получить список БПИФов из {provider.name}")
                
                health_status = provider.health_check()
                if health_status == DataSourceStatus.UNAVAILABLE:
                    errors[provider.name] = Exception(f"Provider {provider.name} is unavailable")
                    continue
                
                securities_list = provider.get_securities_list()
                
                if securities_list:
                    result = FallbackResult(
                        data={'securities': securities_list, 'count': len(securities_list)},
                        source=provider.name,
                        quality_score=1.0 if len(securities_list) > 5 else 0.5,
                        is_cached=False,
                        fallback_level=fallback_level,
                        warnings=[],
                        metadata={
                            'provider_priority': provider.priority,
                            'health_status': health_status.value,
                            'securities_count': len(securities_list)
                        }
                    )
                    
                    logger.info(f"Список БПИФов получен из {provider.name}: {len(securities_list)} инструментов")
                    return result
                else:
                    errors[provider.name] = Exception(f"Empty securities list from {provider.name}")
                    
            except Exception as e:
                logger.error(f"Ошибка получения списка БПИФов из {provider.name}: {e}")
                errors[provider.name] = e
                continue
        
        logger.error("Все провайдеры не смогли предоставить список БПИФов")
        raise AllProvidersUnavailableError(errors)
    
    def get_macro_data_with_fallback(self) -> FallbackResult:
        """
        Получение макроэкономических данных (в основном от CBR)
        
        Returns:
            FallbackResult: Макроэкономические данные
        """
        # Для макроданных используем в первую очередь CBR
        cbr_provider = next((p for p in self.providers if p.name == 'cbr'), None)
        
        if cbr_provider and cbr_provider.enabled:
            try:
                logger.debug("Получение макроэкономических данных из CBR")
                
                health_status = cbr_provider.health_check()
                if health_status != DataSourceStatus.UNAVAILABLE:
                    macro_data = cbr_provider.get_macro_indicators()
                    
                    if macro_data:
                        result = FallbackResult(
                            data=macro_data,
                            source=cbr_provider.name,
                            quality_score=0.9,  # CBR - официальный источник
                            is_cached=False,
                            fallback_level=0,
                            warnings=[],
                            metadata={
                                'provider_priority': cbr_provider.priority,
                                'health_status': health_status.value,
                                'data_type': 'macro_economic'
                            }
                        )
                        
                        logger.info("Макроэкономические данные получены из CBR")
                        return result
                        
            except Exception as e:
                logger.error(f"Ошибка получения макроданных из CBR: {e}")
        
        # Если CBR недоступен, возвращаем заглушку
        logger.warning("CBR недоступен, возвращаем ограниченные макроданные")
        return FallbackResult(
            data={
                'currency_rates': {},
                'key_rate': None,
                'note': 'Макроэкономические данные недоступны - CBR API не отвечает'
            },
            source='fallback',
            quality_score=0.1,
            is_cached=False,
            fallback_level=99,
            warnings=['Макроэкономические данные недоступны'],
            metadata={'data_type': 'macro_economic_fallback'}
        )
    
    def _collect_comprehensive_etf_data(self, provider: DataProvider, ticker: str) -> Dict:
        """
        Сбор комплексных данных о БПИФе от конкретного провайдера
        """
        etf_data = {}
        
        try:
            # Основные рыночные данные
            market_data = provider.get_market_data(ticker)
            if market_data:
                etf_data.update(market_data)
            
            # Исторические данные (доходность, волатильность)
            historical_data = provider.get_historical_data(ticker)
            if historical_data:
                etf_data.update(historical_data)
            
            # Данные об объемах торгов
            volume_data = provider.get_trading_volume_data(ticker)
            if volume_data:
                etf_data.update(volume_data)
            
            # Добавляем метаданные
            etf_data['data_collection_timestamp'] = datetime.now().isoformat()
            etf_data['data_source'] = provider.name
            
            return etf_data
            
        except Exception as e:
            logger.error(f"Ошибка сбора данных о {ticker} из {provider.name}: {e}")
            return {}
    
    def _assess_data_quality(self, data: Dict) -> float:
        """
        Простая оценка качества данных
        """
        score = 0.0
        total_checks = 6
        
        # Проверка наличия ключевых полей
        key_fields = [
            'current_price', 'last_price',  # Цена
            'return_period', 'return_annualized',  # Доходность
            'volatility',  # Волатильность
            'avg_daily_volume',  # Объемы
            'ticker',  # Тикер
            'source'  # Источник
        ]
        
        for field in key_fields:
            if data.get(field) is not None:
                score += 1
        
        return min(1.0, score / total_checks)
    
    def get_provider_status(self) -> Dict:
        """
        Получение статуса всех провайдеров
        """
        status = {
            'providers': [],
            'total_providers': len(self.providers),
            'active_providers': 0,
            'check_timestamp': datetime.now().isoformat()
        }
        
        for provider in self.providers:
            health_status = provider.health_check()
            provider_info = {
                'name': provider.name,
                'priority': provider.priority,
                'enabled': provider.enabled,
                'status': health_status.value,
                'metrics': {
                    'response_time_ms': provider.metrics.response_time_ms,
                    'total_requests': provider.metrics.total_requests,
                    'error_count_1h': provider.metrics.error_count_1h
                }
            }
            
            if health_status in [DataSourceStatus.ACTIVE, DataSourceStatus.DEGRADED]:
                status['active_providers'] += 1
            
            status['providers'].append(provider_info)
        
        return status