"""
ETF Data Collector with Fallback - коллектор данных о российских БПИФах с fallback системой
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import time

from fallback_manager import DataProviderManager
from fallback_system import AllProvidersUnavailableError
from config import KNOWN_ETFS
from logger_config import logger, log_performance


class ETFDataCollectorWithFallback:
    """
    Коллектор данных о российских БПИФах с использованием fallback системы
    Расширяет функциональность существующего BaseETFCollector
    """
    
    def __init__(self):
        self.fallback_manager = DataProviderManager()
        self.known_etfs = KNOWN_ETFS
        
        logger.info("ETF Data Collector с fallback системой инициализирован")
        logger.info(f"Известных БПИФов в базе: {len(self.known_etfs)}")
    
    @log_performance
    def collect_all_etf_data(self) -> pd.DataFrame:
        """
        Сбор данных по всем известным российским БПИФам
        
        Returns:
            pd.DataFrame: Данные по всем БПИФам с метаинформацией
        """
        logger.info("Начинаем сбор данных по всем российским БПИФам")
        
        etf_data_list = []
        successful_collections = 0
        failed_collections = 0
        
        for ticker, metadata in self.known_etfs.items():
            logger.info(f"Обработка БПИФа: {ticker} ({metadata.get('name', 'Неизвестно')})")
            
            try:
                # Получаем данные с fallback
                result = self.fallback_manager.get_etf_data_with_fallback(ticker)
                
                # Объединяем полученные данные с метаданными
                etf_record = self._merge_data_with_metadata(ticker, result.data, metadata)
                
                # Добавляем информацию о fallback
                etf_record.update({
                    'data_source': result.source,
                    'fallback_level': result.fallback_level,
                    'data_quality_score': result.quality_score,
                    'collection_timestamp': datetime.now().isoformat(),
                    'warnings': result.warnings
                })
                
                etf_data_list.append(etf_record)
                successful_collections += 1
                
                logger.info(f"✓ {ticker}: данные получены из {result.source} (качество: {result.quality_score:.2f})")
                
            except AllProvidersUnavailableError as e:
                logger.error(f"✗ {ticker}: все провайдеры недоступны - {e}")
                
                # Создаем запись с минимальными данными
                etf_record = self._create_fallback_record(ticker, metadata)
                etf_data_list.append(etf_record)
                failed_collections += 1
                
            except Exception as e:
                logger.error(f"✗ {ticker}: неожиданная ошибка - {e}")
                
                etf_record = self._create_fallback_record(ticker, metadata)
                etf_data_list.append(etf_record)
                failed_collections += 1
            
            # Небольшая пауза между запросами
            time.sleep(0.5)
        
        # Создаем DataFrame
        df = pd.DataFrame(etf_data_list)
        
        # Рассчитываем рыночные доли
        df = self._calculate_market_shares(df)
        
        # Логируем результаты
        logger.info(f"Сбор данных завершен:")
        logger.info(f"  ✓ Успешно: {successful_collections}")
        logger.info(f"  ✗ Неудачно: {failed_collections}")
        logger.info(f"  📊 Всего БПИФов: {len(df)}")
        
        return df
    
    def collect_etf_data(self, ticker: str) -> Dict:
        """
        Сбор данных по конкретному БПИФу
        
        Args:
            ticker: Тикер БПИФа
            
        Returns:
            Dict: Данные о БПИФе
        """
        logger.info(f"Сбор данных по БПИФу: {ticker}")
        
        try:
            result = self.fallback_manager.get_etf_data_with_fallback(ticker)
            
            metadata = self.known_etfs.get(ticker, {})
            etf_data = self._merge_data_with_metadata(ticker, result.data, metadata)
            
            etf_data.update({
                'data_source': result.source,
                'fallback_level': result.fallback_level,
                'data_quality_score': result.quality_score,
                'collection_timestamp': datetime.now().isoformat()
            })
            
            logger.info(f"Данные по {ticker} получены из {result.source}")
            return etf_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных по {ticker}: {e}")
            return self._create_fallback_record(ticker, self.known_etfs.get(ticker, {}))
    
    def get_etf_list(self) -> List[Dict]:
        """
        Получение списка доступных БПИФов
        
        Returns:
            List[Dict]: Список БПИФов с базовой информацией
        """
        logger.info("Получение списка доступных БПИФов")
        
        try:
            result = self.fallback_manager.get_etf_list_with_fallback()
            
            # Обогащаем список данными из нашей базы знаний
            enriched_list = []
            for security in result.data['securities']:
                ticker = security.get('ticker')
                if ticker in self.known_etfs:
                    security.update(self.known_etfs[ticker])
                enriched_list.append(security)
            
            logger.info(f"Получен список из {len(enriched_list)} БПИФов из {result.source}")
            return enriched_list
            
        except Exception as e:
            logger.error(f"Ошибка получения списка БПИФов: {e}")
            
            # Возвращаем список из нашей базы знаний
            fallback_list = []
            for ticker, metadata in self.known_etfs.items():
                fallback_list.append({
                    'ticker': ticker,
                    'name': metadata.get('name', ticker),
                    'management_company': metadata.get('management_company', 'Неизвестно'),
                    'category': metadata.get('category', 'Неизвестно'),
                    'source': 'fallback_knowledge_base'
                })
            
            logger.info(f"Возвращен fallback список из {len(fallback_list)} БПИФов")
            return fallback_list
    
    def get_macro_data(self) -> Dict:
        """
        Получение макроэкономических данных
        
        Returns:
            Dict: Макроэкономические показатели
        """
        logger.info("Получение макроэкономических данных")
        
        try:
            result = self.fallback_manager.get_macro_data_with_fallback()
            
            macro_data = result.data.copy()
            macro_data.update({
                'data_source': result.source,
                'quality_score': result.quality_score,
                'collection_timestamp': datetime.now().isoformat()
            })
            
            logger.info(f"Макроэкономические данные получены из {result.source}")
            return macro_data
            
        except Exception as e:
            logger.error(f"Ошибка получения макроэкономических данных: {e}")
            return {
                'currency_rates': {},
                'key_rate': None,
                'error': str(e),
                'data_source': 'error',
                'collection_timestamp': datetime.now().isoformat()
            }
    
    def get_provider_status(self) -> Dict:
        """
        Получение статуса всех провайдеров данных
        
        Returns:
            Dict: Статус провайдеров
        """
        return self.fallback_manager.get_provider_status()
    
    def _merge_data_with_metadata(self, ticker: str, api_data: Dict, metadata: Dict) -> Dict:
        """
        Объединение данных из API с метаданными
        """
        merged_data = {
            'ticker': ticker,
            'name': metadata.get('name', ticker),
            'management_company': metadata.get('management_company', 'Неизвестно'),
            'category': metadata.get('category', 'Неизвестно'),
            'tracking_index': metadata.get('tracking_index', 'Неизвестно'),
            'inception_year': metadata.get('inception_year'),
            'expense_ratio': metadata.get('expense_ratio'),
            'benchmark': metadata.get('benchmark')
        }
        
        # Добавляем данные из API
        merged_data.update(api_data)
        
        # Нормализуем ключевые поля
        merged_data['current_price'] = (
            api_data.get('current_price') or 
            api_data.get('last_price') or 
            api_data.get('regularMarketPrice')
        )
        
        merged_data['annual_return'] = (
            api_data.get('return_annualized') or 
            api_data.get('return_1y') or 
            api_data.get('return_period')
        )
        
        merged_data['volatility'] = api_data.get('volatility')
        merged_data['daily_volume'] = api_data.get('avg_daily_volume')
        merged_data['daily_value_rub'] = api_data.get('avg_daily_value_rub')
        
        return merged_data
    
    def _create_fallback_record(self, ticker: str, metadata: Dict) -> Dict:
        """
        Создание записи с минимальными данными при недоступности API
        """
        return {
            'ticker': ticker,
            'name': metadata.get('name', ticker),
            'management_company': metadata.get('management_company', 'Неизвестно'),
            'category': metadata.get('category', 'Неизвестно'),
            'tracking_index': metadata.get('tracking_index', 'Неизвестно'),
            'inception_year': metadata.get('inception_year'),
            'expense_ratio': metadata.get('expense_ratio'),
            'benchmark': metadata.get('benchmark'),
            'current_price': None,
            'annual_return': None,
            'volatility': None,
            'daily_volume': None,
            'daily_value_rub': None,
            'data_source': 'fallback_metadata',
            'fallback_level': 99,
            'data_quality_score': 0.1,
            'collection_timestamp': datetime.now().isoformat(),
            'warnings': ['Данные недоступны из всех источников'],
            'data_availability': 'metadata_only'
        }
    
    def _calculate_market_shares(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Расчет рыночных долей управляющих компаний
        """
        logger.info("Расчет рыночных долей управляющих компаний")
        
        # Фильтруем записи с доступными данными об объемах
        valid_data = df[
            (df['daily_value_rub'].notna()) & 
            (df['daily_value_rub'] > 0)
        ].copy()
        
        if len(valid_data) == 0:
            logger.warning("Нет данных об объемах для расчета рыночных долей")
            df['market_share_percent'] = None
            return df
        
        # Рассчитываем приблизительную рыночную капитализацию
        # Используем дневные объемы торгов как прокси
        valid_data['estimated_market_cap'] = valid_data['daily_value_rub'] * 30  # Примерная оценка
        
        # Группируем по управляющим компаниям
        uk_totals = valid_data.groupby('management_company')['estimated_market_cap'].sum()
        total_market = uk_totals.sum()
        
        if total_market > 0:
            uk_shares = (uk_totals / total_market * 100).round(1)
            
            # Добавляем информацию о долях рынка
            df['market_share_percent'] = df['management_company'].map(uk_shares)
            
            logger.info("Рыночные доли рассчитаны:")
            for uk, share in uk_shares.items():
                logger.info(f"  {uk}: {share}%")
        else:
            df['market_share_percent'] = None
        
        return df
    
    @log_performance
    def create_comprehensive_report(self) -> Dict:
        """
        Создание комплексного отчета по российским БПИФам
        
        Returns:
            Dict: Комплексный отчет
        """
        logger.info("Создание комплексного отчета по российским БПИФам")
        
        # Собираем данные
        etf_df = self.collect_all_etf_data()
        macro_data = self.get_macro_data()
        provider_status = self.get_provider_status()
        
        # Анализируем данные
        report = {
            'report_metadata': {
                'creation_date': datetime.now().isoformat(),
                'total_etfs_analyzed': len(etf_df),
                'data_sources_used': etf_df['data_source'].value_counts().to_dict(),
                'average_data_quality': etf_df['data_quality_score'].mean()
            },
            
            'market_overview': {
                'total_etfs': len(etf_df),
                'management_companies': etf_df['management_company'].nunique(),
                'categories': etf_df['category'].value_counts().to_dict(),
                'market_shares': etf_df.groupby('management_company')['market_share_percent'].first().dropna().to_dict()
            },
            
            'performance_analysis': self._analyze_performance(etf_df),
            'risk_analysis': self._analyze_risk(etf_df),
            'liquidity_analysis': self._analyze_liquidity(etf_df),
            'cost_analysis': self._analyze_costs(etf_df),
            
            'macro_environment': macro_data,
            'data_quality_report': self._create_data_quality_report(etf_df),
            'provider_status': provider_status
        }
        
        logger.info("Комплексный отчет создан")
        return report
    
    def _analyze_performance(self, df: pd.DataFrame) -> Dict:
        """Анализ доходности БПИФов"""
        valid_returns = df[df['annual_return'].notna()]
        
        if len(valid_returns) == 0:
            return {'note': 'Данные о доходности недоступны'}
        
        return {
            'average_return': valid_returns['annual_return'].mean(),
            'median_return': valid_returns['annual_return'].median(),
            'best_performer': {
                'ticker': valid_returns.loc[valid_returns['annual_return'].idxmax(), 'ticker'],
                'return': valid_returns['annual_return'].max(),
                'name': valid_returns.loc[valid_returns['annual_return'].idxmax(), 'name']
            },
            'worst_performer': {
                'ticker': valid_returns.loc[valid_returns['annual_return'].idxmin(), 'ticker'],
                'return': valid_returns['annual_return'].min(),
                'name': valid_returns.loc[valid_returns['annual_return'].idxmin(), 'name']
            },
            'positive_returns_count': (valid_returns['annual_return'] > 0).sum(),
            'negative_returns_count': (valid_returns['annual_return'] < 0).sum()
        }
    
    def _analyze_risk(self, df: pd.DataFrame) -> Dict:
        """Анализ рисков БПИФов"""
        valid_volatility = df[df['volatility'].notna()]
        
        if len(valid_volatility) == 0:
            return {'note': 'Данные о волатильности недоступны'}
        
        return {
            'average_volatility': valid_volatility['volatility'].mean(),
            'median_volatility': valid_volatility['volatility'].median(),
            'lowest_risk': {
                'ticker': valid_volatility.loc[valid_volatility['volatility'].idxmin(), 'ticker'],
                'volatility': valid_volatility['volatility'].min(),
                'name': valid_volatility.loc[valid_volatility['volatility'].idxmin(), 'name']
            },
            'highest_risk': {
                'ticker': valid_volatility.loc[valid_volatility['volatility'].idxmax(), 'ticker'],
                'volatility': valid_volatility['volatility'].max(),
                'name': valid_volatility.loc[valid_volatility['volatility'].idxmax(), 'name']
            }
        }
    
    def _analyze_liquidity(self, df: pd.DataFrame) -> Dict:
        """Анализ ликвидности БПИФов"""
        valid_volume = df[df['daily_volume'].notna()]
        
        if len(valid_volume) == 0:
            return {'note': 'Данные об объемах торгов недоступны'}
        
        return {
            'average_daily_volume': valid_volume['daily_volume'].mean(),
            'median_daily_volume': valid_volume['daily_volume'].median(),
            'most_liquid': {
                'ticker': valid_volume.loc[valid_volume['daily_volume'].idxmax(), 'ticker'],
                'volume': valid_volume['daily_volume'].max(),
                'name': valid_volume.loc[valid_volume['daily_volume'].idxmax(), 'name']
            },
            'least_liquid': {
                'ticker': valid_volume.loc[valid_volume['daily_volume'].idxmin(), 'ticker'],
                'volume': valid_volume['daily_volume'].min(),
                'name': valid_volume.loc[valid_volume['daily_volume'].idxmin(), 'name']
            }
        }
    
    def _analyze_costs(self, df: pd.DataFrame) -> Dict:
        """Анализ комиссий БПИФов"""
        valid_costs = df[df['expense_ratio'].notna()]
        
        if len(valid_costs) == 0:
            return {'note': 'Данные о комиссиях недоступны'}
        
        return {
            'average_expense_ratio': valid_costs['expense_ratio'].mean(),
            'median_expense_ratio': valid_costs['expense_ratio'].median(),
            'lowest_cost': {
                'ticker': valid_costs.loc[valid_costs['expense_ratio'].idxmin(), 'ticker'],
                'expense_ratio': valid_costs['expense_ratio'].min(),
                'name': valid_costs.loc[valid_costs['expense_ratio'].idxmin(), 'name']
            },
            'highest_cost': {
                'ticker': valid_costs.loc[valid_costs['expense_ratio'].idxmax(), 'ticker'],
                'expense_ratio': valid_costs['expense_ratio'].max(),
                'name': valid_costs.loc[valid_costs['expense_ratio'].idxmax(), 'name']
            }
        }
    
    def _create_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """Создание отчета о качестве данных"""
        return {
            'total_records': len(df),
            'records_with_price_data': df['current_price'].notna().sum(),
            'records_with_return_data': df['annual_return'].notna().sum(),
            'records_with_volume_data': df['daily_volume'].notna().sum(),
            'records_with_volatility_data': df['volatility'].notna().sum(),
            'average_quality_score': df['data_quality_score'].mean(),
            'data_sources_distribution': df['data_source'].value_counts().to_dict(),
            'fallback_usage': {
                'primary_source_usage': (df['fallback_level'] == 0).sum(),
                'secondary_source_usage': (df['fallback_level'] == 1).sum(),
                'fallback_only_usage': (df['fallback_level'] >= 2).sum()
            }
        }