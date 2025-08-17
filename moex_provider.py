"""
MOEX Data Provider - реализация провайдера данных для Московской биржи
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fallback_system import DataProvider, DataSourceStatus
from config import config
from logger_config import logger


class MOEXDataProvider(DataProvider):
    """
    Провайдер данных для Московской биржи (MOEX)
    Основной источник данных с высоким приоритетом
    """
    
    def __init__(self, config_dict: Dict = None):
        super().__init__(
            name="moex",
            priority=1,
            config=config_dict or {
                'base_url': 'https://iss.moex.com/iss',
                'timeout': 10,
                'retry_attempts': 3,
                'rate_limit_delay': 0.5
            }
        )
        
        # Инициализация HTTP сессии
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Кэш для проверки здоровья
        self._last_health_check = None
        self._health_check_result = DataSourceStatus.UNKNOWN
        self._health_check_ttl = 300  # 5 minutes
    
    def _make_request_with_retry(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Выполнение запроса с повторными попытками и улучшенной обработкой ошибок
        """
        start_time = time.time()
        last_exception = None
        
        for attempt in range(self.config.get('retry_attempts', 3)):
            try:
                logger.debug(f"MOEX API запрос к {url}, попытка {attempt + 1}")
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.get('timeout', 10)
                )
                
                # Проверка статуса ответа
                response.raise_for_status()
                
                # Проверка содержимого ответа
                if not response.content:
                    raise requests.exceptions.RequestException("Empty response content")
                
                # Парсинг JSON
                try:
                    data = response.json()
                except ValueError as e:
                    raise requests.exceptions.RequestException(f"Invalid JSON response: {e}")
                
                # Обновление метрик успешного запроса
                response_time = (time.time() - start_time) * 1000
                self.update_metrics(response_time, success=True)
                
                # Задержка между запросами для соблюдения rate limit
                time.sleep(self.config.get('rate_limit_delay', 0.5))
                
                logger.debug(f"MOEX API запрос успешен за {response_time:.1f}ms")
                return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"MOEX API timeout (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"MOEX API connection error (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response.status_code >= 500:
                    # Серверные ошибки - повторяем
                    logger.warning(f"MOEX API server error {e.response.status_code} (попытка {attempt + 1}): {e}")
                else:
                    # Клиентские ошибки - не повторяем
                    logger.error(f"MOEX API client error {e.response.status_code}: {e}")
                    break
                    
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"MOEX API request error (попытка {attempt + 1}): {e}")
            
            # Экспоненциальная задержка между попытками
            if attempt < self.config.get('retry_attempts', 3) - 1:
                delay = (2 ** attempt) * 0.5  # 0.5, 1.0, 2.0 seconds
                logger.debug(f"Ожидание {delay}s перед следующей попыткой")
                time.sleep(delay)
        
        # Все попытки исчерпаны
        response_time = (time.time() - start_time) * 1000
        self.update_metrics(response_time, success=False)
        
        logger.error(f"MOEX API: все попытки исчерпаны для {url}. Последняя ошибка: {last_exception}")
        return None
    
    def health_check(self) -> DataSourceStatus:
        """
        Проверка состояния MOEX API
        """
        # Используем кэшированный результат если он свежий
        now = datetime.now()
        if (self._last_health_check and 
            (now - self._last_health_check).total_seconds() < self._health_check_ttl):
            return self._health_check_result
        
        try:
            # Простой запрос для проверки доступности
            url = f"{self.config['base_url']}/index.json"
            start_time = time.time()
            
            response = self.session.get(url, timeout=5)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                if response_time < 1000:  # < 1 second
                    self._health_check_result = DataSourceStatus.ACTIVE
                elif response_time < 5000:  # < 5 seconds
                    self._health_check_result = DataSourceStatus.DEGRADED
                else:
                    self._health_check_result = DataSourceStatus.DEGRADED
                    
                logger.debug(f"MOEX health check: {self._health_check_result.value} ({response_time:.1f}ms)")
            else:
                self._health_check_result = DataSourceStatus.UNAVAILABLE
                logger.warning(f"MOEX health check failed: HTTP {response.status_code}")
                
        except Exception as e:
            self._health_check_result = DataSourceStatus.UNAVAILABLE
            logger.warning(f"MOEX health check failed: {e}")
        
        self._last_health_check = now
        return self._health_check_result
    
    def get_securities_list(self) -> List[Dict]:
        """
        Получение списка ETF с MOEX
        """
        url = f"{self.config['base_url']}/engines/stock/markets/shares/boards/TQTF/securities.json"
        data = self._make_request_with_retry(url)
        
        if not data:
            logger.error("MOEX: не удалось получить список ценных бумаг")
            return []
        
        securities = []
        try:
            if 'securities' in data and 'data' in data['securities']:
                columns = data['securities']['columns']
                
                for row in data['securities']['data']:
                    security_info = dict(zip(columns, row))
                    
                    # Фильтруем только ETF
                    if security_info.get('SECTYPE') == 'ETF':
                        securities.append({
                            'ticker': security_info.get('SECID'),
                            'name': security_info.get('SHORTNAME'),
                            'full_name': security_info.get('SECNAME'),
                            'lot_size': security_info.get('LOTSIZE'),
                            'currency': security_info.get('FACEUNIT', 'RUB'),
                            'source': 'MOEX'
                        })
            
            logger.info(f"MOEX: получено {len(securities)} ETF")
            return securities
            
        except Exception as e:
            logger.error(f"MOEX: ошибка парсинга списка ценных бумаг: {e}")
            return []
    
    def get_market_data(self, ticker: str) -> Dict:
        """
        Получение рыночных данных по тикеру
        """
        url = f"{self.config['base_url']}/engines/stock/markets/shares/securities/{ticker}.json"
        data = self._make_request_with_retry(url)
        
        if not data:
            logger.warning(f"MOEX: не удалось получить рыночные данные для {ticker}")
            return {}
        
        try:
            market_data = {}
            
            # Парсинг данных из securities секции
            if 'securities' in data and 'data' in data['securities'] and data['securities']['data']:
                columns = data['securities']['columns']
                security_data = dict(zip(columns, data['securities']['data'][0]))
                
                market_data.update({
                    'ticker': ticker,
                    'last_price': security_data.get('PREVPRICE'),
                    'market_cap': security_data.get('ISSUESIZE'),
                    'lot_size': security_data.get('LOTSIZE'),
                    'currency': security_data.get('FACEUNIT', 'RUB'),
                    'trading_status': security_data.get('TRADINGSTATUS'),
                    'list_level': security_data.get('LISTLEVEL')
                })
            
            # Парсинг данных из marketdata секции (если есть)
            if 'marketdata' in data and 'data' in data['marketdata'] and data['marketdata']['data']:
                columns = data['marketdata']['columns']
                market_info = dict(zip(columns, data['marketdata']['data'][0]))
                
                market_data.update({
                    'current_price': market_info.get('LAST'),
                    'bid': market_info.get('BID'),
                    'ask': market_info.get('OFFER'),
                    'volume': market_info.get('VOLTODAY'),
                    'value': market_info.get('VALTODAY'),
                    'change': market_info.get('CHANGE'),
                    'change_percent': market_info.get('LASTTOPREVPRICE')
                })
            
            # Добавляем метаданные
            market_data.update({
                'source': 'MOEX',
                'timestamp': datetime.now().isoformat(),
                'data_quality': self._assess_market_data_quality(market_data)
            })
            
            logger.debug(f"MOEX: получены рыночные данные для {ticker}")
            return market_data
            
        except Exception as e:
            logger.error(f"MOEX: ошибка парсинга рыночных данных для {ticker}: {e}")
            return {}
    
    def get_historical_data(self, ticker: str, days: int = 365) -> Dict:
        """
        Получение исторических данных для расчета доходности и волатильности
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        url = f"{self.config['base_url']}/engines/stock/markets/shares/securities/{ticker}/candles.json"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'till': end_date.strftime('%Y-%m-%d'),
            'interval': 24  # Дневные свечи
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"MOEX: не удалось получить исторические данные для {ticker}")
            return {}
        
        try:
            historical_metrics = {}
            
            if 'candles' in data and 'data' in data['candles'] and data['candles']['data']:
                columns = data['candles']['columns']
                df = pd.DataFrame(data['candles']['data'], columns=columns)
                
                if len(df) >= 2:
                    # Конвертация дат
                    df['begin'] = pd.to_datetime(df['begin'])
                    df = df.sort_values('begin')
                    
                    # Расчет доходности
                    first_price = df['close'].iloc[0]
                    last_price = df['close'].iloc[-1]
                    
                    if first_price and last_price and first_price > 0:
                        return_period = (last_price / first_price - 1) * 100
                        
                        # Расчет волатильности
                        df['daily_return'] = df['close'].pct_change()
                        volatility = df['daily_return'].std() * (252 ** 0.5) * 100
                        
                        # Максимальная просадка
                        df['cumulative'] = (1 + df['daily_return']).cumprod()
                        df['running_max'] = df['cumulative'].expanding().max()
                        df['drawdown'] = (df['cumulative'] / df['running_max'] - 1) * 100
                        max_drawdown = df['drawdown'].min()
                        
                        historical_metrics = {
                            'ticker': ticker,
                            'return_period': round(return_period, 2),
                            'return_annualized': round(return_period * (365 / days), 2) if days != 365 else round(return_period, 2),
                            'volatility': round(volatility, 2),
                            'max_drawdown': round(max_drawdown, 2),
                            'first_price': first_price,
                            'last_price': last_price,
                            'data_points': len(df),
                            'period_days': days,
                            'source': 'MOEX',
                            'timestamp': datetime.now().isoformat()
                        }
            
            logger.debug(f"MOEX: получены исторические данные для {ticker} за {days} дней")
            return historical_metrics
            
        except Exception as e:
            logger.error(f"MOEX: ошибка расчета исторических данных для {ticker}: {e}")
            return {}
    
    def get_trading_volume_data(self, ticker: str) -> Dict:
        """
        Получение данных об объемах торгов за последние 30 дней
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        url = f"{self.config['base_url']}/engines/stock/markets/shares/securities/{ticker}/candles.json"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'till': end_date.strftime('%Y-%m-%d'),
            'interval': 24
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"MOEX: не удалось получить данные об объемах для {ticker}")
            return {}
        
        try:
            volume_metrics = {}
            
            if 'candles' in data and 'data' in data['candles'] and data['candles']['data']:
                columns = data['candles']['columns']
                df = pd.DataFrame(data['candles']['data'], columns=columns)
                
                if len(df) > 0:
                    # Фильтруем дни с торгами (объем > 0)
                    trading_days = df[df['volume'] > 0]
                    
                    volume_metrics = {
                        'ticker': ticker,
                        'avg_daily_volume': round(df['volume'].mean(), 0) if 'volume' in df.columns else 0,
                        'avg_daily_value_rub': round(df['value'].mean(), 0) if 'value' in df.columns else 0,
                        'max_daily_volume': round(df['volume'].max(), 0) if 'volume' in df.columns else 0,
                        'trading_days': len(trading_days),
                        'total_days': len(df),
                        'liquidity_score': len(trading_days) / len(df) if len(df) > 0 else 0,
                        'source': 'MOEX',
                        'timestamp': datetime.now().isoformat()
                    }
            
            logger.debug(f"MOEX: получены данные об объемах для {ticker}")
            return volume_metrics
            
        except Exception as e:
            logger.error(f"MOEX: ошибка расчета объемов торгов для {ticker}: {e}")
            return {}
    
    def get_source_info(self) -> Dict:
        """
        Получение информации об источнике данных
        """
        return {
            'name': self.name,
            'display_name': 'Московская биржа',
            'type': 'official_api',
            'base_url': self.config['base_url'],
            'priority': self.priority,
            'data_quality': 'high',
            'real_time': True,
            'historical_depth_years': 10,
            'supported_instruments': ['stocks', 'etf', 'bonds'],
            'rate_limit': '100 requests/minute',
            'documentation': 'https://iss.moex.com/iss/reference/',
            'status': self.health_check().value,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None
        }
    
    def _assess_market_data_quality(self, data: Dict) -> float:
        """
        Оценка качества рыночных данных
        """
        score = 0.0
        total_checks = 5
        
        # Проверка наличия цены
        if data.get('last_price') or data.get('current_price'):
            score += 1
        
        # Проверка наличия объема
        if data.get('volume', 0) > 0:
            score += 1
        
        # Проверка наличия bid/ask
        if data.get('bid') and data.get('ask'):
            score += 1
        
        # Проверка статуса торгов
        if data.get('trading_status'):
            score += 1
        
        # Проверка свежести данных (в рамках торгового дня)
        if data.get('timestamp'):
            score += 1
        
        return score / total_checks