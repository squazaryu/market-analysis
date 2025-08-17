"""
Базовый класс для сбора данных о российских БПИФах
"""

import requests
import pandas as pd
import json
import pickle
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod
import time

from config import config, KNOWN_ETFS
from logger_config import logger, log_performance

class BaseETFCollector(ABC):
    """
    Базовый класс для сбора данных о ETF
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.cache_dir = config.get_file_path("cache", "")
        
    def _get_cache_path(self, cache_key: str) -> str:
        """Получение пути к кэш-файлу"""
        return os.path.join(self.cache_dir, f"{cache_key}.pkl")
    
    def _is_cache_valid(self, cache_path: str) -> bool:
        """Проверка валидности кэша"""
        if not os.path.exists(cache_path):
            return False
        
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
        cache_expiry = datetime.now() - timedelta(hours=config.data.cache_ttl_hours)
        
        return file_time > cache_expiry
    
    def _load_from_cache(self, cache_key: str) -> Optional[Any]:
        """Загрузка данных из кэша"""
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                logger.info(f"Данные загружены из кэша: {cache_key}")
                return data
            except Exception as e:
                logger.warning(f"Ошибка загрузки кэша {cache_key}: {e}")
        
        return None
    
    def _save_to_cache(self, cache_key: str, data: Any) -> None:
        """Сохранение данных в кэш"""
        cache_path = self._get_cache_path(cache_key)
        
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            logger.debug(f"Данные сохранены в кэш: {cache_key}")
        except Exception as e:
            logger.warning(f"Ошибка сохранения в кэш {cache_key}: {e}")
    
    def _make_request_with_retry(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Выполнение запроса с повторными попытками
        """
        for attempt in range(config.api.retry_attempts):
            try:
                logger.debug(f"Запрос к {url}, попытка {attempt + 1}")
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=config.api.request_timeout
                )
                response.raise_for_status()
                
                # Задержка между запросами
                time.sleep(config.api.rate_limit_delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Ошибка запроса (попытка {attempt + 1}/{config.api.retry_attempts}): {e}")
                
                if attempt < config.api.retry_attempts - 1:
                    time.sleep(config.api.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Все попытки исчерпаны для {url}")
        
        return None
    
    @log_performance
    def get_moex_securities_list(self) -> List[Dict]:
        """
        Получение списка ценных бумаг с MOEX
        """
        cache_key = "moex_securities_etf"
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            return cached_data
        
        url = f"{config.api.moex_base_url}/engines/stock/markets/shares/boards/TQTF/securities.json"
        data = self._make_request_with_retry(url)
        
        if not data:
            logger.error("Не удалось получить список ценных бумаг с MOEX")
            return []
        
        securities = []
        if 'securities' in data and 'data' in data['securities']:
            columns = data['securities']['columns']
            
            for row in data['securities']['data']:
                security_info = dict(zip(columns, row))
                if security_info.get('SECTYPE') == 'ETF':
                    securities.append({
                        'ticker': security_info.get('SECID'),
                        'name': security_info.get('SHORTNAME'),
                        'full_name': security_info.get('SECNAME'),
                        'lot_size': security_info.get('LOTSIZE'),
                        'source': 'MOEX'
                    })
        
        self._save_to_cache(cache_key, securities)
        logger.info(f"Получено {len(securities)} ETF с MOEX")
        
        return securities
    
    @log_performance
    def get_security_market_data(self, ticker: str) -> Dict:
        """
        Получение рыночных данных по тикеру
        """
        cache_key = f"market_data_{ticker}_{datetime.now().strftime('%Y%m%d')}"
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            return cached_data
        
        url = f"{config.api.moex_base_url}/engines/stock/markets/shares/securities/{ticker}.json"
        data = self._make_request_with_retry(url)
        
        if not data:
            logger.warning(f"Не удалось получить рыночные данные для {ticker}")
            return {}
        
        market_data = {}
        if 'securities' in data and 'data' in data['securities'] and data['securities']['data']:
            columns = data['securities']['columns']
            security_data = dict(zip(columns, data['securities']['data'][0]))
            
            market_data = {
                'last_price': security_data.get('PREVPRICE'),
                'market_cap': security_data.get('ISSUESIZE'),
                'lot_size': security_data.get('LOTSIZE'),
                'currency': security_data.get('FACEUNIT', 'RUB')
            }
        
        self._save_to_cache(cache_key, market_data)
        return market_data
    
    @log_performance
    def get_historical_data(self, ticker: str, days: int = 365) -> Dict:
        """
        Получение исторических данных для расчета доходности и волатильности
        """
        cache_key = f"historical_{ticker}_{days}days"
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            return cached_data
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        url = f"{config.api.moex_base_url}/engines/stock/markets/shares/securities/{ticker}/candles.json"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'till': end_date.strftime('%Y-%m-%d'),
            'interval': 24
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"Не удалось получить исторические данные для {ticker}")
            return {}
        
        historical_metrics = {}
        if 'candles' in data and 'data' in data['candles'] and data['candles']['data']:
            columns = data['candles']['columns']
            df = pd.DataFrame(data['candles']['data'], columns=columns)
            df['begin'] = pd.to_datetime(df['begin'])
            df = df.sort_values('begin')
            
            if len(df) >= 2:
                first_price = df['close'].iloc[0]
                last_price = df['close'].iloc[-1]
                
                if first_price and last_price and first_price > 0:
                    # Доходность
                    return_1y = (last_price / first_price - 1) * 100
                    
                    # Волатильность
                    df['daily_return'] = df['close'].pct_change()
                    volatility = df['daily_return'].std() * (252 ** 0.5) * 100
                    
                    historical_metrics = {
                        'return_1y': round(return_1y, 2),
                        'volatility': round(volatility, 2),
                        'first_price': first_price,
                        'last_price': last_price,
                        'data_points': len(df)
                    }
        
        self._save_to_cache(cache_key, historical_metrics)
        return historical_metrics
    
    @abstractmethod
    def collect_comprehensive_data(self) -> pd.DataFrame:
        """
        Абстрактный метод для сбора комплексных данных
        Должен быть реализован в наследующих классах
        """
        pass
    
    def validate_data_quality(self, etf_data: Dict) -> str:
        """
        Оценка качества собранных данных
        """
        score = 0
        total_checks = 4
        
        # Проверка наличия цены
        if etf_data.get('current_price'):
            score += 1
        
        # Проверка наличия доходности
        if etf_data.get('return_1y_percent') is not None:
            score += 1
        
        # Проверка наличия данных об объемах
        if etf_data.get('avg_daily_volume', 0) > 0:
            score += 1
        
        # Проверка метаданных
        if etf_data.get('management_company') and etf_data.get('category'):
            score += 1
        
        quality_ratio = score / total_checks
        
        if quality_ratio >= 0.75:
            return 'Хорошее'
        elif quality_ratio >= 0.5:
            return 'Частичное'
        else:
            return 'Ограниченное'
    
    def enrich_with_metadata(self, ticker: str, base_data: Dict) -> Dict:
        """
        Обогащение данных метаинформацией
        """
        metadata = KNOWN_ETFS.get(ticker, {})
        
        enriched_data = {**base_data, **metadata}
        enriched_data['ticker'] = ticker
        enriched_data['data_collection_date'] = datetime.now().strftime('%Y-%m-%d')
        
        return enriched_data