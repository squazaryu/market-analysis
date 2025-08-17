"""
Исследование API источников для сбора данных о российских БПИФах и финансовых рынках
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time

from logger_config import logger, log_performance

class APIResearcher:
    """
    Класс для исследования и тестирования различных API источников
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.api_results = {}
    
    @log_performance
    def test_moex_api(self) -> Dict[str, Any]:
        """
        Тестирование расширенных возможностей MOEX API
        """
        logger.info("Тестирование MOEX API")
        
        results = {
            'name': 'Moscow Exchange API',
            'base_url': 'https://iss.moex.com/iss',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # 1. Тест базового доступа
            url = f"{results['base_url']}/securities.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'active'
                
                # 2. Проверка доступных рынков
                markets_url = f"{results['base_url']}/engines.json"
                markets_resp = self.session.get(markets_url, timeout=10)
                
                if markets_resp.status_code == 200:
                    markets_data = markets_resp.json()
                    if 'engines' in markets_data:
                        results['capabilities']['markets'] = len(markets_data['engines']['data'])
                
                # 3. Проверка исторических данных
                hist_url = f"{results['base_url']}/engines/stock/markets/shares/securities/SBER/candles.json"
                hist_params = {
                    'from': '2024-01-01',
                    'till': '2024-12-31',
                    'interval': 24
                }
                
                hist_resp = self.session.get(hist_url, params=hist_params, timeout=10)
                if hist_resp.status_code == 200:
                    hist_data = hist_resp.json()
                    if 'candles' in hist_data and hist_data['candles']['data']:
                        results['capabilities']['historical_data'] = len(hist_data['candles']['data'])
                
                # 4. Проверка данных по ETF
                etf_url = f"{results['base_url']}/engines/stock/markets/shares/boards/TQTF/securities.json"
                etf_resp = self.session.get(etf_url, timeout=10)
                
                if etf_resp.status_code == 200:
                    etf_data = etf_resp.json()
                    if 'securities' in etf_data:
                        results['capabilities']['etf_count'] = len(etf_data['securities']['data'])
                
                # 5. Проверка индексов
                index_url = f"{results['base_url']}/engines/stock/markets/index/securities.json"
                index_resp = self.session.get(index_url, timeout=10)
                
                if index_resp.status_code == 200:
                    index_data = index_resp.json()
                    if 'securities' in index_data:
                        results['capabilities']['indices_count'] = len(index_data['securities']['data'])
                
                results['capabilities'].update({
                    'real_time_quotes': True,
                    'historical_candles': True,
                    'trading_volumes': True,
                    'corporate_actions': True,
                    'free_access': True,
                    'rate_limit': '100 req/min',
                    'data_delay': 'Real-time for basic data'
                })
                
                results['limitations'] = {
                    'fundamental_data': False,
                    'analyst_estimates': False,
                    'news_feed': False,
                    'portfolio_analytics': False,
                    'alternative_data': False
                }
                
                results['data_quality'] = 'high'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании MOEX API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_cbr_api(self) -> Dict[str, Any]:
        """
        Тестирование API Центрального банка РФ
        """
        logger.info("Тестирование ЦБ РФ API")
        
        results = {
            'name': 'Central Bank of Russia API',
            'base_url': 'https://www.cbr-xml-daily.ru/api',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # 1. Тест курсов валют
            rates_url = f"{results['base_url']}/latest.js"
            response = self.session.get(rates_url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'active'
                rates_data = response.json()
                
                results['capabilities'].update({
                    'currency_rates': True,
                    'currencies_count': len(rates_data.get('rates', {})),
                    'historical_rates': True,
                    'free_access': True,
                    'update_frequency': 'Daily',
                    'data_format': 'JSON/XML'
                })
                
                # 2. Тест ключевой ставки
                key_rate_url = "https://cbr.ru/hd_base/KeyRate/"
                # Примечание: это HTML страница, нужен парсинг
                
                results['limitations'] = {
                    'stock_data': False,
                    'etf_data': False,
                    'real_time_data': False,
                    'trading_volumes': False,
                    'corporate_data': False
                }
                
                results['data_quality'] = 'high'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании ЦБ РФ API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_finam_api(self) -> Dict[str, Any]:
        """
        Тестирование Finam API
        """
        logger.info("Тестирование Finam API")
        
        results = {
            'name': 'Finam API',
            'base_url': 'https://trade-api.finam.ru',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Finam требует авторизации для большинства данных
            # Тестируем публичные endpoints
            
            public_url = "https://www.finam.ru/profile/moex-stock/sber/export/"
            response = self.session.get(public_url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'limited_public'
                
                results['capabilities'].update({
                    'historical_data': True,
                    'stock_quotes': True,
                    'export_formats': ['CSV', 'TXT', 'Excel'],
                    'intraday_data': True,
                    'requires_auth': True,
                    'professional_api': True
                })
                
                results['limitations'] = {
                    'free_real_time': False,
                    'api_access_paid': True,
                    'rate_limits': 'Varies by subscription',
                    'registration_required': True
                }
                
                results['data_quality'] = 'high'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Finam API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_yahoo_finance_api(self) -> Dict[str, Any]:
        """
        Тестирование Yahoo Finance API (неофициальный)
        """
        logger.info("Тестирование Yahoo Finance API")
        
        results = {
            'name': 'Yahoo Finance API',
            'base_url': 'https://query1.finance.yahoo.com/v8/finance',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Тест для российских акций (некоторые доступны как .ME)
            url = f"{results['base_url']}/chart/SBER.ME"
            params = {
                'interval': '1d',
                'range': '1mo'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'chart' in data and data['chart']['result']:
                    results['status'] = 'active'
                    
                    results['capabilities'].update({
                        'russian_stocks': True,
                        'historical_data': True,
                        'intraday_data': True,
                        'free_access': True,
                        'global_coverage': True,
                        'fundamental_data': True,
                        'options_data': True
                    })
                    
                    results['limitations'] = {
                        'russian_etf_limited': True,
                        'unofficial_api': True,
                        'rate_limits': 'Unclear',
                        'terms_of_service': 'Restrictive',
                        'data_delay': '15-20 minutes'
                    }
                    
                    results['data_quality'] = 'medium'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Yahoo Finance API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_investpy_api(self) -> Dict[str, Any]:
        """
        Тестирование investing.com через investpy
        """
        logger.info("Тестирование Investing.com API")
        
        results = {
            'name': 'Investing.com API (via investpy)',
            'base_url': 'https://api.investing.com',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Прямой доступ к investing.com ограничен
            # Можно использовать библиотеку investpy (если установлена)
            
            try:
                import investpy
                
                # Тест получения данных по российскому рынку
                russian_stocks = investpy.get_stocks_list(country='russia')
                
                if russian_stocks:
                    results['status'] = 'active'
                    
                    results['capabilities'].update({
                        'russian_stocks': True,
                        'stocks_count': len(russian_stocks),
                        'historical_data': True,
                        'fundamental_data': True,
                        'economic_calendar': True,
                        'technical_indicators': True,
                        'free_library': True
                    })
                    
                    results['limitations'] = {
                        'rate_limits': 'Present',
                        'library_dependency': True,
                        'unofficial_api': True,
                        'stability_issues': True
                    }
                    
                    results['data_quality'] = 'medium'
                    
            except ImportError:
                results['status'] = 'library_not_installed'
                results['note'] = 'investpy library required'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Investing.com API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_tinkoff_api(self) -> Dict[str, Any]:
        """
        Тестирование Тинькофф Инвестиции API
        """
        logger.info("Тестирование Tinkoff API")
        
        results = {
            'name': 'Tinkoff Investments API',
            'base_url': 'https://api-invest.tinkoff.ru/openapi',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Тинькофф API требует токен авторизации
            # Тестируем без авторизации для понимания доступности
            
            url = f"{results['base_url']}/market/stocks"
            response = self.session.get(url, timeout=10)
            
            # Ожидаем 401 Unauthorized - это нормально
            if response.status_code == 401:
                results['status'] = 'requires_token'
                
                results['capabilities'].update({
                    'russian_stocks': True,
                    'etf_data': True,
                    'bonds_data': True,
                    'currencies': True,
                    'real_time_data': True,
                    'trading_operations': True,
                    'portfolio_data': True,
                    'orders_management': True,
                    'free_api': True
                })
                
                results['limitations'] = {
                    'token_required': True,
                    'account_needed': True,
                    'rate_limits': 'Present',
                    'personal_use_only': True
                }
                
                results['data_quality'] = 'high'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Tinkoff API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_sber_api(self) -> Dict[str, Any]:
        """
        Тестирование Сбер Инвестор API
        """
        logger.info("Тестирование Sber API")
        
        results = {
            'name': 'Sber Investor API',
            'base_url': 'https://api.sberbank.ru/ru/person',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Сбер API также требует авторизацию
            # Проверяем доступность сервиса
            
            url = "https://www.sberbank.ru/ru/quotes"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'web_interface_available'
                
                results['capabilities'].update({
                    'quotes_web': True,
                    'api_exists': True,
                    'requires_auth': True,
                    'client_access_only': True
                })
                
                results['limitations'] = {
                    'public_api_limited': True,
                    'client_only': True,
                    'documentation_limited': True
                }
                
                results['data_quality'] = 'unknown'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Sber API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_alpha_direct_api(self) -> Dict[str, Any]:
        """
        Тестирование Альфа-Директ API
        """
        logger.info("Тестирование Alfa-Direct API")
        
        results = {
            'name': 'Alfa-Direct API',
            'base_url': 'https://api.alfadirect.ru',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Альфа-Директ имеет собственный API для клиентов
            url = "https://www.alfadirect.ru"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'platform_available'
                
                results['capabilities'].update({
                    'trading_platform': True,
                    'api_for_clients': True,
                    'algorithmic_trading': True,
                    'real_time_data': True
                })
                
                results['limitations'] = {
                    'client_only': True,
                    'paid_access': True,
                    'documentation_private': True
                }
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Alfa-Direct API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_quandl_api(self) -> Dict[str, Any]:
        """
        Тестирование Quandl API (теперь часть Nasdaq)
        """
        logger.info("Тестирование Quandl API")
        
        results = {
            'name': 'Quandl API (Nasdaq Data Link)',
            'base_url': 'https://data.nasdaq.com/api/v3',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # Тест бесплатного доступа к некоторым данным
            url = f"{results['base_url']}/datasets/WIKI/AAPL.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'active'
                
                results['capabilities'].update({
                    'economic_data': True,
                    'alternative_data': True,
                    'global_markets': True,
                    'api_access': True,
                    'python_library': True,
                    'excel_plugin': True
                })
                
                results['limitations'] = {
                    'russian_data_limited': True,
                    'free_tier_limited': True,
                    'rate_limits': '50 calls/day (free)',
                    'premium_required': True
                }
                
                results['data_quality'] = 'high'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании Quandl API: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    @log_performance
    def test_smartlab_api(self) -> Dict[str, Any]:
        """
        Тестирование SmartLab API (неофициальный)
        """
        logger.info("Тестирование SmartLab API")
        
        results = {
            'name': 'SmartLab API (Unofficial)',
            'base_url': 'https://smart-lab.ru',
            'status': 'unknown',
            'capabilities': {},
            'limitations': {},
            'data_quality': 'unknown'
        }
        
        try:
            # SmartLab не имеет официального API, но есть парсинг возможности
            url = f"{results['base_url']}/q/shares_fundamental"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                results['status'] = 'web_scraping_possible'
                
                results['capabilities'].update({
                    'fundamental_data': True,
                    'russian_focus': True,
                    'analytics': True,
                    'community_data': True,
                    'free_access': True
                })
                
                results['limitations'] = {
                    'no_official_api': True,
                    'scraping_required': True,
                    'unstable_structure': True,
                    'legal_considerations': True
                }
                
                results['data_quality'] = 'medium'
                
        except Exception as e:
            logger.error(f"Ошибка при тестировании SmartLab: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results
    
    def run_comprehensive_research(self) -> Dict[str, Any]:
        """
        Запуск комплексного исследования всех API
        """
        logger.info("Запуск комплексного исследования API")
        
        api_tests = [
            ('moex', self.test_moex_api),
            ('cbr', self.test_cbr_api),
            ('finam', self.test_finam_api),
            ('yahoo_finance', self.test_yahoo_finance_api),
            ('investpy', self.test_investpy_api),
            ('tinkoff', self.test_tinkoff_api),
            ('sber', self.test_sber_api),
            ('alfa_direct', self.test_alpha_direct_api),
            ('quandl', self.test_quandl_api),
            ('smartlab', self.test_smartlab_api)
        ]
        
        research_results = {
            'timestamp': datetime.now().isoformat(),
            'total_apis_tested': len(api_tests),
            'apis': {},
            'summary': {}
        }
        
        active_apis = 0
        limited_apis = 0
        error_apis = 0
        
        for api_name, test_func in api_tests:
            logger.info(f"Тестирование {api_name}...")
            
            try:
                result = test_func()
                research_results['apis'][api_name] = result
                
                if result['status'] == 'active':
                    active_apis += 1
                elif result['status'] in ['limited_public', 'requires_token', 'web_interface_available']:
                    limited_apis += 1
                else:
                    error_apis += 1
                    
                # Пауза между запросами
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Критическая ошибка при тестировании {api_name}: {e}")
                research_results['apis'][api_name] = {
                    'name': api_name,
                    'status': 'critical_error',
                    'error': str(e)
                }
                error_apis += 1
        
        research_results['summary'] = {
            'active_apis': active_apis,
            'limited_apis': limited_apis,
            'error_apis': error_apis,
            'success_rate': round((active_apis + limited_apis) / len(api_tests) * 100, 1)
        }
        
        return research_results
    
    def generate_recommendations(self, research_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Генерация рекомендаций на основе результатов исследования
        """
        recommendations = {
            'primary_sources': [],
            'secondary_sources': [],
            'avoid': [],
            'integration_strategy': {},
            'data_coverage_analysis': {}
        }
        
        # Анализ результатов и формирование рекомендаций
        for api_name, api_data in research_results['apis'].items():
            if api_data['status'] == 'active' and api_data.get('data_quality') == 'high':
                recommendations['primary_sources'].append({
                    'name': api_data['name'],
                    'api_key': api_name,
                    'strengths': api_data.get('capabilities', {}),
                    'limitations': api_data.get('limitations', {})
                })
            elif api_data['status'] in ['limited_public', 'requires_token'] and api_data.get('data_quality') in ['high', 'medium']:
                recommendations['secondary_sources'].append({
                    'name': api_data['name'],
                    'api_key': api_name,
                    'access_method': api_data['status'],
                    'potential': api_data.get('capabilities', {})
                })
            elif api_data['status'] in ['error', 'critical_error']:
                recommendations['avoid'].append({
                    'name': api_data['name'],
                    'reason': api_data.get('error', 'Unknown error')
                })
        
        return recommendations

def main():
    """
    Основная функция для запуска исследования API
    """
    logger.info("=== ИССЛЕДОВАНИЕ API ИСТОЧНИКОВ ДАННЫХ ===")
    
    researcher = APIResearcher()
    
    try:
        # Запуск комплексного исследования
        results = researcher.run_comprehensive_research()
        
        # Генерация рекомендаций
        recommendations = researcher.generate_recommendations(results)
        
        # Сохранение результатов
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Сохранение подробных результатов
        with open(f'/Users/tumowuh/Desktop/market analysis/api_research_results_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # Сохранение рекомендаций
        with open(f'/Users/tumowuh/Desktop/market analysis/api_recommendations_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, ensure_ascii=False, indent=2)
        
        # Вывод краткого отчета
        logger.info("=== КРАТКИЙ ОТЧЕТ ===")
        logger.info(f"Протестировано API: {results['total_apis_tested']}")
        logger.info(f"Активных: {results['summary']['active_apis']}")
        logger.info(f"С ограничениями: {results['summary']['limited_apis']}")
        logger.info(f"Недоступных: {results['summary']['error_apis']}")
        logger.info(f"Успешность: {results['summary']['success_rate']}%")
        
        logger.info("\n=== РЕКОМЕНДУЕМЫЕ К ИСПОЛЬЗОВАНИЮ ===")
        for source in recommendations['primary_sources']:
            logger.info(f"✅ {source['name']}")
        
        logger.info("\n=== ДОПОЛНИТЕЛЬНЫЕ ИСТОЧНИКИ ===")
        for source in recommendations['secondary_sources']:
            logger.info(f"🔶 {source['name']} ({source['access_method']})")
        
        return results, recommendations
        
    except Exception as e:
        logger.error(f"Критическая ошибка в исследовании: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()