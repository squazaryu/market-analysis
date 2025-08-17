"""
Yahoo Finance Data Provider - реализация провайдера данных для Yahoo Finance API
"""

import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fallback_system import DataProvider, DataSourceStatus
from logger_config import logger


class YahooFinanceProvider(DataProvider):
    """
    Провайдер данных для Yahoo Finance API
    Вторичный источник данных с поддержкой российских тикеров
    """
    
    def __init__(self, config_dict: Dict = None):
        super().__init__(
            name="yahoo_finance",
            priority=2,
            config=config_dict or {
                'base_url': 'https://query1.finance.yahoo.com/v8/finance',
                'timeout': 15,
                'retry_attempts': 2,
                'ticker_suffix': '.ME',  # Moscow Exchange suffix
                'rate_limit_delay': 1.0  # More conservative for unofficial API
            }
        )
        
        # Инициализация HTTP сессии
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Кэш для проверки здоровья
        self._last_health_check = None
        self._health_check_result = DataSourceStatus.UNKNOWN
        self._health_check_ttl = 300  # 5 minutes
        
        # Маппинг российских тикеров на Yahoo Finance формат
        self.ticker_mapping = {
            'SBER': 'SBER.ME',
            'GAZP': 'GAZP.ME',
            'LKOH': 'LKOH.ME',
            'NVTK': 'NVTK.ME',
            'ROSN': 'ROSN.ME',
            'TATN': 'TATN.ME',
            'GMKN': 'GMKN.ME',
            'MAGN': 'MAGN.ME',
            'NLMK': 'NLMK.ME',
            'ALRS': 'ALRS.ME',
            'CHMF': 'CHMF.ME',
            'MOEX': 'MOEX.ME',
            'RTKM': 'RTKM.ME',
            'AFLT': 'AFLT.ME',
            'MTSS': 'MTSS.ME',
            'YNDX': 'YNDX.ME',
            'OZON': 'OZON.ME',
            'FIVE': 'FIVE.ME',
            'FIXP': 'FIXP.ME',
            'TCSG': 'TCSG.ME',
            # ETF mappings
            'SBMX': 'SBMX.ME',
            'VTBX': 'VTBX.ME',
            'FXRU': 'FXRU.ME',
            'FXUS': 'FXUS.ME',
            'FXGD': 'FXGD.ME',
            'TECH': 'TECH.ME',
            'TGLD': 'TGLD.ME',
            'DIVD': 'DIVD.ME',
            'SBGB': 'SBGB.ME',
            'SBCB': 'SBCB.ME'
        }
    
    def _convert_ticker(self, ticker: str) -> str:
        """
        Конвертация российского тикера в Yahoo Finance формат
        """
        # Проверяем прямое соответствие в маппинге
        if ticker in self.ticker_mapping:
            return self.ticker_mapping[ticker]
        
        # Если тикер уже содержит суффикс, возвращаем как есть
        if '.' in ticker:
            return ticker
        
        # Добавляем стандартный суффикс для российских тикеров
        return f"{ticker}{self.config['ticker_suffix']}"
    
    def _make_request_with_retry(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Выполнение запроса с повторными попытками для неофициального API
        """
        start_time = time.time()
        last_exception = None
        
        for attempt in range(self.config.get('retry_attempts', 2)):
            try:
                logger.debug(f"Yahoo Finance API запрос к {url}, попытка {attempt + 1}")
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.get('timeout', 15)
                )
                
                # Yahoo Finance может возвращать 404 для несуществующих тикеров
                if response.status_code == 404:
                    logger.warning(f"Yahoo Finance: тикер не найден (404)")
                    return None
                
                # Проверка других HTTP ошибок
                response.raise_for_status()
                
                # Проверка содержимого ответа
                if not response.content:
                    raise requests.exceptions.RequestException("Empty response content")
                
                # Парсинг JSON
                try:
                    data = response.json()
                except ValueError as e:
                    raise requests.exceptions.RequestException(f"Invalid JSON response: {e}")
                
                # Проверка на ошибки в ответе Yahoo Finance
                if 'error' in data:
                    raise requests.exceptions.RequestException(f"Yahoo Finance API error: {data['error']}")
                
                # Обновление метрик успешного запроса
                response_time = (time.time() - start_time) * 1000
                self.update_metrics(response_time, success=True)
                
                # Более длительная задержка для неофициального API
                time.sleep(self.config.get('rate_limit_delay', 1.0))
                
                logger.debug(f"Yahoo Finance API запрос успешен за {response_time:.1f}ms")
                return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"Yahoo Finance API timeout (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"Yahoo Finance API connection error (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response.status_code == 429:  # Rate limit
                    logger.warning(f"Yahoo Finance API rate limit (попытка {attempt + 1}): {e}")
                    # Увеличиваем задержку при rate limit
                    time.sleep(5.0)
                elif e.response.status_code >= 500:
                    logger.warning(f"Yahoo Finance API server error {e.response.status_code} (попытка {attempt + 1}): {e}")
                else:
                    logger.error(f"Yahoo Finance API client error {e.response.status_code}: {e}")
                    break
                    
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"Yahoo Finance API request error (попытка {attempt + 1}): {e}")
            
            # Экспоненциальная задержка между попытками
            if attempt < self.config.get('retry_attempts', 2) - 1:
                delay = (2 ** attempt) * 1.0  # 1.0, 2.0 seconds
                logger.debug(f"Ожидание {delay}s перед следующей попыткой")
                time.sleep(delay)
        
        # Все попытки исчерпаны
        response_time = (time.time() - start_time) * 1000
        self.update_metrics(response_time, success=False)
        
        logger.error(f"Yahoo Finance API: все попытки исчерпаны для {url}. Последняя ошибка: {last_exception}")
        return None
    
    def health_check(self) -> DataSourceStatus:
        """
        Проверка состояния Yahoo Finance API
        """
        # Используем кэшированный результат если он свежий
        now = datetime.now()
        if (self._last_health_check and 
            (now - self._last_health_check).total_seconds() < self._health_check_ttl):
            return self._health_check_result
        
        try:
            # Тестовый запрос к популярному тикеру
            url = f"{self.config['base_url']}/chart/SBER.ME"
            params = {
                'interval': '1d',
                'range': '1d'
            }
            
            start_time = time.time()
            response = self.session.get(url, params=params, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Проверяем структуру ответа
                    if 'chart' in data and 'result' in data['chart']:
                        if response_time < 2000:  # < 2 seconds
                            self._health_check_result = DataSourceStatus.ACTIVE
                        elif response_time < 10000:  # < 10 seconds
                            self._health_check_result = DataSourceStatus.DEGRADED
                        else:
                            self._health_check_result = DataSourceStatus.DEGRADED
                    else:
                        self._health_check_result = DataSourceStatus.DEGRADED
                except ValueError:
                    self._health_check_result = DataSourceStatus.DEGRADED
                    
                logger.debug(f"Yahoo Finance health check: {self._health_check_result.value} ({response_time:.1f}ms)")
            else:
                self._health_check_result = DataSourceStatus.UNAVAILABLE
                logger.warning(f"Yahoo Finance health check failed: HTTP {response.status_code}")
                
        except Exception as e:
            self._health_check_result = DataSourceStatus.UNAVAILABLE
            logger.warning(f"Yahoo Finance health check failed: {e}")
        
        self._last_health_check = now
        return self._health_check_result
    
    def get_securities_list(self) -> List[Dict]:
        """
        Получение списка ценных бумаг (ограниченная функциональность для Yahoo Finance)
        """
        # Yahoo Finance не предоставляет прямого API для получения списка российских ETF
        # Возвращаем предопределенный список известных тикеров
        securities = []
        
        for ticker, yahoo_ticker in self.ticker_mapping.items():
            # Проверяем доступность тикера
            try:
                market_data = self.get_market_data(ticker)
                if market_data:
                    securities.append({
                        'ticker': ticker,
                        'yahoo_ticker': yahoo_ticker,
                        'name': market_data.get('shortName', ticker),
                        'full_name': market_data.get('longName', ticker),
                        'currency': market_data.get('currency', 'RUB'),
                        'source': 'Yahoo Finance'
                    })
            except Exception as e:
                logger.debug(f"Yahoo Finance: тикер {ticker} недоступен: {e}")
                continue
        
        logger.info(f"Yahoo Finance: получено {len(securities)} доступных тикеров")
        return securities
    
    def get_market_data(self, ticker: str) -> Dict:
        """
        Получение рыночных данных по тикеру
        """
        yahoo_ticker = self._convert_ticker(ticker)
        url = f"{self.config['base_url']}/chart/{yahoo_ticker}"
        
        params = {
            'interval': '1d',
            'range': '1d',
            'includePrePost': 'false'
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"Yahoo Finance: не удалось получить рыночные данные для {ticker}")
            return {}
        
        try:
            market_data = {}
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                meta = result.get('meta', {})
                
                # Основные данные
                market_data.update({
                    'ticker': ticker,
                    'yahoo_ticker': yahoo_ticker,
                    'current_price': meta.get('regularMarketPrice'),
                    'previous_close': meta.get('previousClose'),
                    'currency': meta.get('currency', 'RUB'),
                    'exchange': meta.get('exchangeName'),
                    'market_state': meta.get('marketState'),
                    'timezone': meta.get('timezone')
                })
                
                # Дополнительные данные если доступны
                if 'indicators' in result and 'quote' in result['indicators']:
                    quotes = result['indicators']['quote'][0]
                    timestamps = result.get('timestamp', [])
                    
                    if timestamps and quotes.get('close'):
                        # Берем последние доступные данные
                        last_idx = -1
                        while last_idx >= -len(quotes['close']) and quotes['close'][last_idx] is None:
                            last_idx -= 1
                        
                        if abs(last_idx) <= len(quotes['close']):
                            market_data.update({
                                'last_price': quotes['close'][last_idx],
                                'high': quotes['high'][last_idx] if quotes.get('high') else None,
                                'low': quotes['low'][last_idx] if quotes.get('low') else None,
                                'volume': quotes['volume'][last_idx] if quotes.get('volume') else None,
                                'open': quotes['open'][last_idx] if quotes.get('open') else None
                            })
                
                # Расчет изменения цены
                current = market_data.get('current_price') or market_data.get('last_price')
                previous = market_data.get('previous_close')
                
                if current and previous and previous > 0:
                    change = current - previous
                    change_percent = (change / previous) * 100
                    market_data.update({
                        'change': round(change, 2),
                        'change_percent': round(change_percent, 2)
                    })
            
            # Добавляем метаданные
            market_data.update({
                'source': 'Yahoo Finance',
                'timestamp': datetime.now().isoformat(),
                'data_quality': self._assess_market_data_quality(market_data),
                'data_delay_minutes': 15  # Yahoo Finance has ~15 min delay
            })
            
            logger.debug(f"Yahoo Finance: получены рыночные данные для {ticker}")
            return market_data
            
        except Exception as e:
            logger.error(f"Yahoo Finance: ошибка парсинга рыночных данных для {ticker}: {e}")
            return {}
    
    def get_historical_data(self, ticker: str, days: int = 365) -> Dict:
        """
        Получение исторических данных
        """
        yahoo_ticker = self._convert_ticker(ticker)
        
        # Yahoo Finance использует период в формате "1y", "6mo", etc.
        if days <= 7:
            period = "7d"
        elif days <= 30:
            period = "1mo"
        elif days <= 90:
            period = "3mo"
        elif days <= 180:
            period = "6mo"
        elif days <= 365:
            period = "1y"
        elif days <= 730:
            period = "2y"
        else:
            period = "5y"
        
        url = f"{self.config['base_url']}/chart/{yahoo_ticker}"
        params = {
            'interval': '1d',
            'range': period
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"Yahoo Finance: не удалось получить исторические данные для {ticker}")
            return {}
        
        try:
            historical_metrics = {}
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                timestamps = result.get('timestamp', [])
                
                if 'indicators' in result and 'quote' in result['indicators']:
                    quotes = result['indicators']['quote'][0]
                    closes = quotes.get('close', [])
                    
                    # Фильтруем None значения
                    valid_closes = [(i, price) for i, price in enumerate(closes) if price is not None]
                    
                    if len(valid_closes) >= 2:
                        first_price = valid_closes[0][1]
                        last_price = valid_closes[-1][1]
                        
                        # Расчет доходности
                        return_period = (last_price / first_price - 1) * 100
                        
                        # Расчет волатильности
                        prices = [price for _, price in valid_closes]
                        returns = []
                        for i in range(1, len(prices)):
                            if prices[i-1] > 0:
                                daily_return = (prices[i] / prices[i-1]) - 1
                                returns.append(daily_return)
                        
                        if returns:
                            import statistics
                            volatility = statistics.stdev(returns) * (252 ** 0.5) * 100
                            
                            # Максимальная просадка
                            cumulative = [1]
                            for ret in returns:
                                cumulative.append(cumulative[-1] * (1 + ret))
                            
                            running_max = []
                            current_max = cumulative[0]
                            for val in cumulative:
                                current_max = max(current_max, val)
                                running_max.append(current_max)
                            
                            drawdowns = [(cum / run_max - 1) * 100 for cum, run_max in zip(cumulative, running_max)]
                            max_drawdown = min(drawdowns) if drawdowns else 0
                            
                            historical_metrics = {
                                'ticker': ticker,
                                'yahoo_ticker': yahoo_ticker,
                                'return_period': round(return_period, 2),
                                'return_annualized': round(return_period * (365 / days), 2) if days != 365 else round(return_period, 2),
                                'volatility': round(volatility, 2),
                                'max_drawdown': round(max_drawdown, 2),
                                'first_price': first_price,
                                'last_price': last_price,
                                'data_points': len(valid_closes),
                                'period_days': days,
                                'source': 'Yahoo Finance',
                                'timestamp': datetime.now().isoformat(),
                                'data_delay_minutes': 15
                            }
            
            logger.debug(f"Yahoo Finance: получены исторические данные для {ticker} за {days} дней")
            return historical_metrics
            
        except Exception as e:
            logger.error(f"Yahoo Finance: ошибка расчета исторических данных для {ticker}: {e}")
            return {}
    
    def get_trading_volume_data(self, ticker: str) -> Dict:
        """
        Получение данных об объемах торгов
        """
        yahoo_ticker = self._convert_ticker(ticker)
        url = f"{self.config['base_url']}/chart/{yahoo_ticker}"
        
        params = {
            'interval': '1d',
            'range': '1mo'  # Последний месяц
        }
        
        data = self._make_request_with_retry(url, params)
        
        if not data:
            logger.warning(f"Yahoo Finance: не удалось получить данные об объемах для {ticker}")
            return {}
        
        try:
            volume_metrics = {}
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                
                if 'indicators' in result and 'quote' in result['indicators']:
                    quotes = result['indicators']['quote'][0]
                    volumes = quotes.get('volume', [])
                    closes = quotes.get('close', [])
                    
                    # Фильтруем дни с торгами
                    valid_volumes = [vol for vol in volumes if vol is not None and vol > 0]
                    valid_data = [(vol, close) for vol, close in zip(volumes, closes) 
                                 if vol is not None and close is not None and vol > 0]
                    
                    if valid_data:
                        volumes_only = [vol for vol, _ in valid_data]
                        closes_only = [close for _, close in valid_data]
                        
                        # Расчет средней стоимости торгов (приблизительно)
                        avg_values = [vol * close for vol, close in valid_data]
                        
                        volume_metrics = {
                            'ticker': ticker,
                            'yahoo_ticker': yahoo_ticker,
                            'avg_daily_volume': round(sum(volumes_only) / len(volumes_only), 0) if volumes_only else 0,
                            'avg_daily_value_rub': round(sum(avg_values) / len(avg_values), 0) if avg_values else 0,
                            'max_daily_volume': round(max(volumes_only), 0) if volumes_only else 0,
                            'trading_days': len(valid_data),
                            'total_days': len([v for v in volumes if v is not None]),
                            'liquidity_score': len(valid_data) / len([v for v in volumes if v is not None]) if volumes else 0,
                            'source': 'Yahoo Finance',
                            'timestamp': datetime.now().isoformat(),
                            'data_delay_minutes': 15
                        }
            
            logger.debug(f"Yahoo Finance: получены данные об объемах для {ticker}")
            return volume_metrics
            
        except Exception as e:
            logger.error(f"Yahoo Finance: ошибка расчета объемов торгов для {ticker}: {e}")
            return {}
    
    def get_source_info(self) -> Dict:
        """
        Получение информации об источнике данных
        """
        return {
            'name': self.name,
            'display_name': 'Yahoo Finance',
            'type': 'unofficial_api',
            'base_url': self.config['base_url'],
            'priority': self.priority,
            'data_quality': 'medium',
            'real_time': False,
            'data_delay_minutes': 15,
            'historical_depth_years': 5,
            'supported_instruments': ['stocks', 'etf'],
            'rate_limit': 'Unofficial - conservative usage',
            'documentation': 'https://finance.yahoo.com/',
            'status': self.health_check().value,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'ticker_mapping_count': len(self.ticker_mapping),
            'limitations': [
                'Неофициальный API',
                'Задержка данных ~15 минут',
                'Ограниченное покрытие российских ETF',
                'Возможны блокировки при частых запросах'
            ]
        }
    
    def _assess_market_data_quality(self, data: Dict) -> float:
        """
        Оценка качества рыночных данных для Yahoo Finance
        """
        score = 0.0
        total_checks = 5
        
        # Проверка наличия цены
        if data.get('current_price') or data.get('last_price'):
            score += 1
        
        # Проверка наличия изменения цены
        if data.get('change') is not None:
            score += 1
        
        # Проверка наличия объема (может отсутствовать для некоторых инструментов)
        if data.get('volume'):
            score += 0.5  # Половина балла, так как не всегда доступно
        
        # Проверка валюты
        if data.get('currency'):
            score += 0.5
        
        # Проверка свежести данных (учитываем задержку Yahoo Finance)
        if data.get('timestamp'):
            score += 1
        
        # Проверка наличия high/low данных
        if data.get('high') and data.get('low'):
            score += 1
        
        return min(1.0, score / total_checks)