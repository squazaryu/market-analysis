"""
API Fallback System - Core interfaces and base classes
Система резервных источников данных для анализа российских БПИФов
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime


class DataSourceStatus(Enum):
    """Статус источника данных"""
    ACTIVE = "active"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


@dataclass
class FallbackResult:
    """Результат запроса с метаданными о fallback"""
    data: Dict
    source: str
    quality_score: float
    is_cached: bool
    cache_age_hours: Optional[float] = None
    fallback_level: int = 0  # 0 = primary, 1 = secondary, etc.
    warnings: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class FallbackConfig:
    """Конфигурация системы fallback"""
    providers: List[Dict] = field(default_factory=list)
    health_check_interval: int = 300  # 5 minutes
    cache_ttl_hours: int = 24
    max_cache_age_hours: int = 168  # 1 week
    quality_threshold: float = 0.7
    notification_settings: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.notification_settings:
            self.notification_settings = {
                'email_alerts': False,
                'telegram_alerts': False,
                'log_level': 'WARNING'
            }


@dataclass
class ProviderMetrics:
    """Метрики производительности провайдера"""
    status: DataSourceStatus
    response_time_ms: float
    success_rate_24h: float
    last_successful_request: Optional[datetime] = None
    error_count_1h: int = 0
    total_requests: int = 0
    cache_hit_rate: float = 0.0


class DataProvider(ABC):
    """
    Абстрактный базовый класс для всех провайдеров данных
    Определяет унифицированный интерфейс для работы с различными API
    """
    
    def __init__(self, name: str, priority: int = 1, config: Dict = None):
        self.name = name
        self.priority = priority
        self.config = config or {}
        self.enabled = True
        self.metrics = ProviderMetrics(
            status=DataSourceStatus.UNKNOWN,
            response_time_ms=0.0,
            success_rate_24h=0.0
        )
    
    @abstractmethod
    def get_securities_list(self) -> List[Dict]:
        """
        Получение списка ценных бумаг
        
        Returns:
            List[Dict]: Список словарей с информацией о ценных бумагах
        """
        pass
    
    @abstractmethod
    def get_market_data(self, ticker: str) -> Dict:
        """
        Получение рыночных данных по тикеру
        
        Args:
            ticker: Тикер ценной бумаги
            
        Returns:
            Dict: Рыночные данные (цена, объем, капитализация и т.д.)
        """
        pass
    
    @abstractmethod
    def get_historical_data(self, ticker: str, days: int = 365) -> Dict:
        """
        Получение исторических данных
        
        Args:
            ticker: Тикер ценной бумаги
            days: Количество дней истории
            
        Returns:
            Dict: Исторические данные (доходность, волатильность и т.д.)
        """
        pass
    
    @abstractmethod
    def get_trading_volume_data(self, ticker: str) -> Dict:
        """
        Получение данных об объемах торгов
        
        Args:
            ticker: Тикер ценной бумаги
            
        Returns:
            Dict: Данные об объемах торгов
        """
        pass
    
    @abstractmethod
    def health_check(self) -> DataSourceStatus:
        """
        Проверка состояния источника данных
        
        Returns:
            DataSourceStatus: Текущий статус источника
        """
        pass
    
    @abstractmethod
    def get_source_info(self) -> Dict:
        """
        Получение информации об источнике данных
        
        Returns:
            Dict: Метаинформация об источнике
        """
        pass
    
    def is_available(self) -> bool:
        """Проверка доступности провайдера"""
        return self.enabled and self.health_check() in [
            DataSourceStatus.ACTIVE, 
            DataSourceStatus.DEGRADED
        ]
    
    def update_metrics(self, response_time: float, success: bool):
        """Обновление метрик производительности"""
        self.metrics.response_time_ms = response_time
        self.metrics.total_requests += 1
        
        if success:
            self.metrics.last_successful_request = datetime.now()
        else:
            self.metrics.error_count_1h += 1


class FallbackSystemError(Exception):
    """Базовое исключение системы fallback"""
    pass


class AllProvidersUnavailableError(FallbackSystemError):
    """Все провайдеры недоступны"""
    def __init__(self, provider_errors: Dict[str, Exception]):
        self.provider_errors = provider_errors
        super().__init__(f"All providers failed: {list(provider_errors.keys())}")


class DataQualityError(FallbackSystemError):
    """Качество данных ниже порогового значения"""
    def __init__(self, quality_score: float, threshold: float):
        self.quality_score = quality_score
        self.threshold = threshold
        super().__init__(f"Data quality {quality_score:.2f} below threshold {threshold:.2f}")


class CacheExpiredError(FallbackSystemError):
    """Кэш устарел и нет доступных провайдеров"""
    def __init__(self, cache_age_hours: float, max_age_hours: float):
        self.cache_age_hours = cache_age_hours
        self.max_age_hours = max_age_hours
        super().__init__(f"Cache expired: {cache_age_hours:.1f}h > {max_age_hours:.1f}h")


# Конфигурация провайдеров по умолчанию
DEFAULT_FALLBACK_PROVIDERS = [
    {
        'name': 'moex',
        'class': 'MOEXDataProvider',
        'priority': 1,
        'enabled': True,
        'config': {
            'base_url': 'https://iss.moex.com/iss',
            'timeout': 10,
            'retry_attempts': 3,
            'rate_limit_delay': 0.5
        }
    },
    {
        'name': 'yahoo_finance',
        'class': 'YahooFinanceProvider',
        'priority': 2,
        'enabled': True,
        'config': {
            'base_url': 'https://query1.finance.yahoo.com/v8/finance',
            'timeout': 15,
            'retry_attempts': 2,
            'ticker_suffix': '.ME'
        }
    },
    {
        'name': 'cbr',
        'class': 'CBRDataProvider',
        'priority': 3,
        'enabled': True,
        'config': {
            'base_url': 'https://www.cbr-xml-daily.ru/api',
            'data_types': ['currency_rates', 'key_rate'],
            'timeout': 10
        }
    },
    {
        'name': 'cache',
        'class': 'CacheProvider',
        'priority': 99,  # Lowest priority - last resort
        'enabled': True,
        'config': {
            'max_age_hours': 168,  # 1 week
            'quality_degradation_factor': 0.8
        }
    }
]