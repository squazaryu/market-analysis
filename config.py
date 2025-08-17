"""
Конфигурация для анализа российских БПИФов
"""

import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class APIConfig:
    """Настройки для работы с API"""
    moex_base_url: str = "https://iss.moex.com/iss"
    request_timeout: int = 10
    retry_attempts: int = 3
    retry_delay: float = 0.5
    rate_limit_delay: float = 0.5

@dataclass
class DataConfig:
    """Настройки для работы с данными"""
    cache_dir: str = "cache"
    cache_ttl_hours: int = 24
    data_dir: str = "data"
    reports_dir: str = "reports"
    visualizations_dir: str = "visualizations"

@dataclass
class AnalysisConfig:
    """Настройки для анализа"""
    min_data_quality_threshold: float = 0.7
    volatility_calculation_days: int = 252
    performance_periods: List[str] = None
    default_currency: str = "RUB"
    
    def __post_init__(self):
        if self.performance_periods is None:
            self.performance_periods = ["1m", "3m", "6m", "1y"]

class Config:
    """Главный класс конфигурации"""
    
    def __init__(self, base_path: str = None):
        self.base_path = base_path or "/Users/tumowuh/Desktop/market analysis"
        self.api = APIConfig()
        self.data = DataConfig()
        self.analysis = AnalysisConfig()
        
        # Создаем необходимые директории
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Создание необходимых директорий"""
        dirs_to_create = [
            self.data.cache_dir,
            self.data.data_dir,
            self.data.reports_dir,
            self.data.visualizations_dir
        ]
        
        for dir_name in dirs_to_create:
            dir_path = os.path.join(self.base_path, dir_name)
            os.makedirs(dir_path, exist_ok=True)
    
    def get_file_path(self, category: str, filename: str) -> str:
        """Получение полного пути к файлу"""
        category_map = {
            "cache": self.data.cache_dir,
            "data": self.data.data_dir,
            "reports": self.data.reports_dir,
            "visualizations": self.data.visualizations_dir
        }
        
        if category not in category_map:
            raise ValueError(f"Unknown category: {category}")
        
        return os.path.join(self.base_path, category_map[category], filename)

# Глобальная конфигурация
config = Config()

# Известные ETF с метаданными
KNOWN_ETFS = {
    'SBMX': {
        'name': 'Сбер - Мосбиржа',
        'management_company': 'Сбер Управление Активами',
        'category': 'Российские акции',
        'tracking_index': 'Индекс МосБиржи',
        'inception_year': 2021,
        'expense_ratio': 0.95,
        'benchmark': 'IMOEX'
    },
    'VTBX': {
        'name': 'ВТБ - Мосбиржа',
        'management_company': 'ВТБ Капитал',
        'category': 'Российские акции',
        'tracking_index': 'Индекс МосБиржи',
        'inception_year': 2020,
        'expense_ratio': 0.89,
        'benchmark': 'IMOEX'
    },
    'TECH': {
        'name': 'Тинькофф - Технологии США',
        'management_company': 'Тинькофф Капитал',
        'category': 'Технологические акции',
        'tracking_index': 'NASDAQ-100',
        'inception_year': 2021,
        'expense_ratio': 1.15,
        'benchmark': 'NDX'
    },
    'TGLD': {
        'name': 'Тинькофф - Золото',
        'management_company': 'Тинькофф Капитал',
        'category': 'Драгоценные металлы',
        'tracking_index': 'Gold Spot',
        'inception_year': 2021,
        'expense_ratio': 0.85,
        'benchmark': 'GOLD'
    },
    'FXRU': {
        'name': 'FinEx Россия',
        'management_company': 'FinEx',
        'category': 'Российские акции',
        'tracking_index': 'MSCI Russia',
        'inception_year': 2015,
        'expense_ratio': 0.99,
        'benchmark': 'MSCI Russia'
    },
    'FXUS': {
        'name': 'FinEx США',
        'management_company': 'FinEx',
        'category': 'Зарубежные акции',
        'tracking_index': 'S&P 500',
        'inception_year': 2013,
        'expense_ratio': 0.90,
        'benchmark': 'S&P 500'
    },
    'FXGD': {
        'name': 'FinEx Золото',
        'management_company': 'FinEx',
        'category': 'Драгоценные металлы',
        'tracking_index': 'Gold Spot',
        'inception_year': 2015,
        'expense_ratio': 0.85,
        'benchmark': 'GOLD'
    },
    'DIVD': {
        'name': 'Альфа - Дивиденды',
        'management_company': 'УК Альфа-Капитал',
        'category': 'Дивидендные акции',
        'tracking_index': 'Дивидендный индекс',
        'inception_year': 2020,
        'expense_ratio': 1.05,
        'benchmark': 'Dividend Index'
    },
    'SBGB': {
        'name': 'Сбер - ОФЗ',
        'management_company': 'Сбер Управление Активами',
        'category': 'Государственные облигации',
        'tracking_index': 'Индекс ОФЗ',
        'inception_year': 2020,
        'expense_ratio': 0.45,
        'benchmark': 'RGBI'
    },
    'SBCB': {
        'name': 'Сбер - Корпоративные облигации',
        'management_company': 'Сбер Управление Активами',
        'category': 'Корпоративные облигации',
        'tracking_index': 'Индекс корп. облигаций',
        'inception_year': 2020,
        'expense_ratio': 0.65,
        'benchmark': 'RCBI'
    }
}