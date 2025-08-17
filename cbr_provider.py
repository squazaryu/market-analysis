"""
Central Bank of Russia Data Provider - реализация провайдера данных для ЦБ РФ
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fallback_system import DataProvider, DataSourceStatus
from logger_config import logger


class CBRDataProvider(DataProvider):
    """
    Провайдер данных для ЦБ РФ
    Специализируется на макроэкономических данных: валютные курсы, ключевая ставка
    """
    
    def __init__(self, config_dict: Dict = None):
        super().__init__(
            name="cbr",
            priority=3,
            config=config_dict or {
                'base_url': 'https://www.cbr-xml-daily.ru/api',
                'timeout': 10,
                'retry_attempts': 3,
                'data_types': ['currency_rates', 'key_rate'],
                'rate_limit_delay': 0.5
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
        
        # Поддерживаемые валюты
        self.supported_currencies = {
            'USD': 'Доллар США',
            'EUR': 'Евро',
            'GBP': 'Фунт стерлингов',
            'JPY': 'Японская иена',
            'CNY': 'Китайский юань',
            'CHF': 'Швейцарский франк',
            'CAD': 'Канадский доллар',
            'AUD': 'Австралийский доллар',
            'KZT': 'Казахстанский тенге',
            'BYN': 'Белорусский рубль',
            'UAH': 'Украинская гривна'
        }
    
    def _make_request_with_retry(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Выполнение запроса с повторными попытками для ЦБ РФ API
        """
        start_time = time.time()
        last_exception = None
        
        for attempt in range(self.config.get('retry_attempts', 3)):
            try:
                logger.debug(f"CBR API запрос к {url}, попытка {attempt + 1}")
                
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
                
                # Задержка между запросами
                time.sleep(self.config.get('rate_limit_delay', 0.5))
                
                logger.debug(f"CBR API запрос успешен за {response_time:.1f}ms")
                return data
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"CBR API timeout (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"CBR API connection error (попытка {attempt + 1}): {e}")
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response.status_code >= 500:
                    logger.warning(f"CBR API server error {e.response.status_code} (попытка {attempt + 1}): {e}")
                else:
                    logger.error(f"CBR API client error {e.response.status_code}: {e}")
                    break
                    
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"CBR API request error (попытка {attempt + 1}): {e}")
            
            # Задержка между попытками
            if attempt < self.config.get('retry_attempts', 3) - 1:
                delay = (attempt + 1) * 1.0  # 1.0, 2.0, 3.0 seconds
                logger.debug(f"Ожидание {delay}s перед следующей попыткой")
                time.sleep(delay)
        
        # Все попытки исчерпаны
        response_time = (time.time() - start_time) * 1000
        self.update_metrics(response_time, success=False)
        
        logger.error(f"CBR API: все попытки исчерпаны для {url}. Последняя ошибка: {last_exception}")
        return None
    
    def health_check(self) -> DataSourceStatus:
        """
        Проверка состояния CBR API
        """
        # Используем кэшированный результат если он свежий
        now = datetime.now()
        if (self._last_health_check and 
            (now - self._last_health_check).total_seconds() < self._health_check_ttl):
            return self._health_check_result
        
        try:
            # Простой запрос для проверки доступности
            url = f"{self.config['base_url']}/latest.js"
            start_time = time.time()
            
            response = self.session.get(url, timeout=5)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Проверяем структуру ответа
                    if 'Valute' in data and isinstance(data['Valute'], dict):
                        if response_time < 2000:  # < 2 seconds
                            self._health_check_result = DataSourceStatus.ACTIVE
                        elif response_time < 5000:  # < 5 seconds
                            self._health_check_result = DataSourceStatus.DEGRADED
                        else:
                            self._health_check_result = DataSourceStatus.DEGRADED
                    else:
                        self._health_check_result = DataSourceStatus.DEGRADED
                except ValueError:
                    self._health_check_result = DataSourceStatus.DEGRADED
                    
                logger.debug(f"CBR health check: {self._health_check_result.value} ({response_time:.1f}ms)")
            else:
                self._health_check_result = DataSourceStatus.UNAVAILABLE
                logger.warning(f"CBR health check failed: HTTP {response.status_code}")
                
        except Exception as e:
            self._health_check_result = DataSourceStatus.UNAVAILABLE
            logger.warning(f"CBR health check failed: {e}")
        
        self._last_health_check = now
        return self._health_check_result
    
    def get_securities_list(self) -> List[Dict]:
        """
        ЦБ РФ не предоставляет данные о ценных бумагах
        Возвращаем пустой список с предупреждением
        """
        logger.info("CBR: API не поддерживает получение списка ценных бумаг")
        return []
    
    def get_market_data(self, ticker: str) -> Dict:
        """
        ЦБ РФ не предоставляет рыночные данные по тикерам
        Возвращаем пустой словарь с предупреждением
        """
        logger.info(f"CBR: API не поддерживает рыночные данные для тикера {ticker}")
        return {}
    
    def get_historical_data(self, ticker: str, days: int = 365) -> Dict:
        """
        ЦБ РФ не предоставляет исторические данные по тикерам
        Возвращаем пустой словарь с предупреждением
        """
        logger.info(f"CBR: API не поддерживает исторические данные для тикера {ticker}")
        return {}
    
    def get_trading_volume_data(self, ticker: str) -> Dict:
        """
        ЦБ РФ не предоставляет данные об объемах торгов
        Возвращаем пустой словарь с предупреждением
        """
        logger.info(f"CBR: API не поддерживает данные об объемах для тикера {ticker}")
        return {}
    
    def get_currency_rates(self, date: Optional[str] = None) -> Dict:
        """
        Получение курсов валют на указанную дату
        
        Args:
            date: Дата в формате YYYY-MM-DD (по умолчанию - сегодня)
            
        Returns:
            Dict: Курсы валют
        """
        if date:
            # Исторические курсы
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                url = f"{self.config['base_url']}/archive/{date_obj.strftime('%Y/%m/%d')}.js"
            except ValueError:
                logger.error(f"CBR: неверный формат даты {date}, ожидается YYYY-MM-DD")
                return {}
        else:
            # Текущие курсы
            url = f"{self.config['base_url']}/latest.js"
        
        data = self._make_request_with_retry(url)
        
        if not data:
            logger.warning(f"CBR: не удалось получить курсы валют на {date or 'сегодня'}")
            return {}
        
        try:
            currency_rates = {
                'date': data.get('Date'),
                'timestamp': data.get('Timestamp'),
                'rates': {},
                'source': 'CBR',
                'api_timestamp': datetime.now().isoformat()
            }
            
            if 'Valute' in data:
                for currency_code, currency_data in data['Valute'].items():
                    if currency_code in self.supported_currencies:
                        currency_rates['rates'][currency_code] = {
                            'name': currency_data.get('Name'),
                            'nominal': currency_data.get('Nominal'),
                            'value': currency_data.get('Value'),
                            'previous': currency_data.get('Previous'),
                            'change': currency_data.get('Value', 0) - currency_data.get('Previous', 0) if currency_data.get('Value') and currency_data.get('Previous') else None
                        }
            
            logger.debug(f"CBR: получены курсы {len(currency_rates['rates'])} валют на {date or 'сегодня'}")
            return currency_rates
            
        except Exception as e:
            logger.error(f"CBR: ошибка парсинга курсов валют: {e}")
            return {}
    
    def get_currency_dynamics(self, currency: str, days: int = 30) -> Dict:
        """
        Получение динамики курса валюты за период
        
        Args:
            currency: Код валюты (USD, EUR, etc.)
            days: Количество дней для анализа
            
        Returns:
            Dict: Динамика курса валюты
        """
        if currency not in self.supported_currencies:
            logger.warning(f"CBR: валюта {currency} не поддерживается")
            return {}
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Получаем курсы за период
        rates_history = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Пропускаем выходные (ЦБ не публикует курсы)
            if current_date.weekday() < 5:  # 0-4 = Monday-Friday
                rates_data = self.get_currency_rates(date_str)
                
                if rates_data and currency in rates_data.get('rates', {}):
                    rate_info = rates_data['rates'][currency]
                    rates_history.append({
                        'date': date_str,
                        'value': rate_info['value'],
                        'nominal': rate_info['nominal']
                    })
            
            current_date += timedelta(days=1)
            
            # Небольшая задержка чтобы не перегружать API
            time.sleep(0.1)
        
        if not rates_history:
            logger.warning(f"CBR: не удалось получить историю курса {currency}")
            return {}
        
        try:
            # Анализ динамики
            values = [r['value'] for r in rates_history]
            first_value = values[0]
            last_value = values[-1]
            
            change_absolute = last_value - first_value
            change_percent = (change_absolute / first_value) * 100 if first_value > 0 else 0
            
            # Волатильность
            if len(values) > 1:
                daily_changes = []
                for i in range(1, len(values)):
                    if values[i-1] > 0:
                        daily_change = (values[i] / values[i-1] - 1) * 100
                        daily_changes.append(daily_change)
                
                import statistics
                volatility = statistics.stdev(daily_changes) if len(daily_changes) > 1 else 0
            else:
                volatility = 0
            
            dynamics = {
                'currency': currency,
                'currency_name': self.supported_currencies[currency],
                'period_days': days,
                'data_points': len(rates_history),
                'first_value': first_value,
                'last_value': last_value,
                'min_value': min(values),
                'max_value': max(values),
                'change_absolute': round(change_absolute, 4),
                'change_percent': round(change_percent, 2),
                'volatility_percent': round(volatility, 2),
                'history': rates_history,
                'source': 'CBR',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.debug(f"CBR: получена динамика {currency} за {days} дней")
            return dynamics
            
        except Exception as e:
            logger.error(f"CBR: ошибка анализа динамики {currency}: {e}")
            return {}
    
    def get_key_rate_info(self) -> Dict:
        """
        Получение информации о ключевой ставке ЦБ РФ
        Примечание: ЦБ РФ API не предоставляет прямого доступа к ключевой ставке
        Возвращаем заглушку с информацией о том, что данные недоступны
        """
        logger.info("CBR: API не предоставляет прямого доступа к ключевой ставке")
        
        return {
            'key_rate': None,
            'effective_date': None,
            'previous_rate': None,
            'change': None,
            'note': 'Ключевая ставка недоступна через CBR API. Требуется парсинг сайта ЦБ РФ.',
            'source': 'CBR',
            'timestamp': datetime.now().isoformat(),
            'alternative_source': 'https://cbr.ru/hd_base/KeyRate/'
        }
    
    def get_macro_indicators(self) -> Dict:
        """
        Получение основных макроэкономических индикаторов
        """
        indicators = {
            'currency_rates': self.get_currency_rates(),
            'key_rate': self.get_key_rate_info(),
            'major_currencies_dynamics': {},
            'source': 'CBR',
            'timestamp': datetime.now().isoformat()
        }
        
        # Получаем динамику основных валют за неделю
        major_currencies = ['USD', 'EUR', 'CNY']
        for currency in major_currencies:
            try:
                dynamics = self.get_currency_dynamics(currency, days=7)
                if dynamics:
                    indicators['major_currencies_dynamics'][currency] = {
                        'current_rate': dynamics['last_value'],
                        'change_percent_7d': dynamics['change_percent'],
                        'volatility_7d': dynamics['volatility_percent']
                    }
            except Exception as e:
                logger.warning(f"CBR: не удалось получить динамику {currency}: {e}")
        
        return indicators
    
    def get_source_info(self) -> Dict:
        """
        Получение информации об источнике данных
        """
        return {
            'name': self.name,
            'display_name': 'Центральный банк РФ',
            'type': 'official_api',
            'base_url': self.config['base_url'],
            'priority': self.priority,
            'data_quality': 'high',
            'real_time': False,
            'update_frequency': 'daily',
            'data_scope': 'macro_economic',
            'supported_data_types': self.config.get('data_types', []),
            'supported_currencies': list(self.supported_currencies.keys()),
            'documentation': 'https://www.cbr-xml-daily.ru/',
            'status': self.health_check().value,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'limitations': [
                'Только макроэкономические данные',
                'Нет данных по ценным бумагам',
                'Курсы валют обновляются в рабочие дни',
                'Ключевая ставка недоступна через API'
            ]
        }