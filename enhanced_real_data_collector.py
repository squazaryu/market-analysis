"""
Расширенный сбор реальных данных о российских БПИФах с дополнительной информацией
"""

from base_collector import BaseETFCollector
from config import config, KNOWN_ETFS
from logger_config import logger, log_performance
import pandas as pd
from datetime import datetime
import time
from typing import Dict

class EnhancedRealETFCollector(BaseETFCollector):
    """
    Расширенный класс для сбора реальных данных о российских БПИФах
    Наследуется от BaseETFCollector с добавленным функционалом
    """
    
    def __init__(self):
        super().__init__()
        
    def get_trading_volume_data(self, ticker: str) -> Dict:
        """
        Получение данных об объемах торгов с кэшированием
        """
        cache_key = f"volume_data_{ticker}_{datetime.now().strftime('%Y%m%d')}"
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            return cached_data
            
        from datetime import datetime, timedelta
        
        url = f"{config.api.moex_base_url}/engines/stock/markets/shares/securities/{ticker}/candles.json"
        params = {
            'from': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'till': datetime.now().strftime('%Y-%m-%d'),
            'interval': 24
        }
        
        data = self._make_request_with_retry(url, params)
        
        volume_metrics = {}
        if data and 'candles' in data and 'data' in data['candles'] and data['candles']['data']:
            columns = data['candles']['columns']
            df = pd.DataFrame(data['candles']['data'], columns=columns)
            
            volume_metrics = {
                'avg_daily_volume': round(df['volume'].mean(), 0) if 'volume' in df.columns else 0,
                'avg_daily_value_rub': round(df['value'].mean(), 0) if 'value' in df.columns else 0
            }
        
        self._save_to_cache(cache_key, volume_metrics)
        return volume_metrics
    
    @log_performance
    def collect_comprehensive_data(self) -> pd.DataFrame:
        """
        Реализация сбора комплексных данных
        """
        logger.info("Начинаем сбор комплексных данных о БПИФах")
        
        enriched_data = []
        
        for ticker, details in KNOWN_ETFS.items():
            logger.info(f"Обработка {ticker}...")
            
            try:
                # Получаем все типы данных
                current_data = self.get_security_market_data(ticker)
                historical_data = self.get_historical_data(ticker)
                volume_data = self.get_trading_volume_data(ticker)
                
                # Объединяем данные
                etf_record = {
                    'ticker': ticker,
                    'current_price': current_data.get('last_price'),
                    'market_cap_rub': current_data.get('market_cap'),
                    'return_1y_percent': historical_data.get('return_1y'),
                    'volatility_percent': historical_data.get('volatility'),
                    'avg_daily_volume': volume_data.get('avg_daily_volume'),
                    'avg_daily_value_rub': volume_data.get('avg_daily_value_rub'),
                }
                
                # Обогащаем метаданными
                enriched_record = self.enrich_with_metadata(ticker, etf_record)
                enriched_record['data_quality'] = self.validate_data_quality(enriched_record)
                
                enriched_data.append(enriched_record)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке {ticker}: {e}")
                # Создаем запись с минимальными данными
                enriched_record = self.enrich_with_metadata(ticker, {
                    'ticker': ticker,
                    'data_quality': 'Ограниченное'
                })
                enriched_data.append(enriched_record)
        
        df = pd.DataFrame(enriched_data)
        logger.info(f"Собрано данных о {len(df)} ETF")
        
        return self.calculate_market_shares(df)
    
    def enrich_etf_data(self):
        """
        Обогащение данных ETF дополнительной информацией
        """
        print("Обогащение данных ETF...")
        
        # Известные ETF с дополнительной информацией
        etf_details = {
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
        
        enriched_data = []
        
        for ticker, details in etf_details.items():
            print(f"Обработка {ticker}...")
            
            # Получаем текущие рыночные данные
            current_data = self.get_current_market_data(ticker)
            
            # Получаем исторические данные для расчета доходности
            historical_data = self.get_historical_data(ticker)
            
            # Получаем данные об объемах
            volume_data = self.get_trading_volume_data(ticker)
            
            # Объединяем все данные
            etf_record = {
                'ticker': ticker,
                'name': details['name'],
                'management_company': details['management_company'],
                'category': details['category'],
                'tracking_index': details['tracking_index'],
                'inception_year': details['inception_year'],
                'expense_ratio': details['expense_ratio'],
                'current_price': current_data.get('last_price'),
                'market_cap_rub': current_data.get('market_cap'),
                'return_1y_percent': historical_data.get('return_1y'),
                'volatility_percent': historical_data.get('volatility'),
                'avg_daily_volume': volume_data.get('avg_daily_volume'),
                'avg_daily_value_rub': volume_data.get('avg_daily_value_rub'),
                'data_collection_date': datetime.now().strftime('%Y-%m-%d'),
                'data_quality': self.assess_data_quality(current_data, historical_data, volume_data)
            }
            
            enriched_data.append(etf_record)
            time.sleep(0.5)  # Пауза между запросами
            
        return pd.DataFrame(enriched_data)
    
    def get_current_market_data(self, ticker):
        """
        Получение текущих рыночных данных
        """
        try:
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'securities' in data and 'data' in data['securities']:
                    sec_data = data['securities']['data']
                    columns = data['securities']['columns']
                    
                    if sec_data:
                        market_info = dict(zip(columns, sec_data[0]))
                        
                        return {
                            'last_price': market_info.get('PREVPRICE'),
                            'market_cap': market_info.get('ISSUESIZE'),
                            'lot_size': market_info.get('LOTSIZE'),
                            'currency': market_info.get('FACEUNIT', 'RUB')
                        }
            
            return {}
            
        except Exception as e:
            print(f"Ошибка получения текущих данных для {ticker}: {e}")
            return {}
    
    def assess_data_quality(self, current_data, historical_data, volume_data):
        """
        Оценка качества собранных данных
        """
        score = 0
        
        if current_data.get('last_price'):
            score += 1
        if historical_data.get('return_1y') is not None:
            score += 1
        if volume_data.get('avg_daily_volume', 0) > 0:
            score += 1
            
        if score >= 2:
            return 'Хорошее'
        elif score == 1:
            return 'Частичное'
        else:
            return 'Ограниченное'
    
    def calculate_market_shares(self, df):
        """
        Расчет рыночных долей на основе реальных данных
        """
        print("Расчет рыночных долей...")
        
        # Фильтруем записи с ценами и объемами
        valid_data = df[df['current_price'].notna() & df['avg_daily_value_rub'].notna()].copy()
        
        if len(valid_data) == 0:
            print("Недостаточно данных для расчета рыночных долей")
            return df
        
        # Рассчитываем приблизительную капитализацию на основе дневных объемов торгов
        # Это приблизительная оценка, так как точные данные об активах под управлением недоступны
        valid_data['estimated_market_cap'] = valid_data['avg_daily_value_rub'] * 30  # Примерная оценка
        
        # Рассчитываем доли рынка по УК
        uk_totals = valid_data.groupby('management_company')['estimated_market_cap'].sum()
        total_market = uk_totals.sum()
        
        if total_market > 0:
            uk_shares = (uk_totals / total_market * 100).round(1)
            
            # Добавляем информацию о долях рынка обратно в DataFrame
            df['uk_market_share_percent'] = df['management_company'].map(uk_shares)
        
        return df
    
    def create_real_market_analysis(self):
        """
        Создание реального анализа рынка
        """
        print("=== СОЗДАНИЕ РЕАЛЬНОГО АНАЛИЗА РЫНКА БПИФов ===\n")
        
        # Получаем обогащенные данные
        df = self.enrich_etf_data()
        
        # Рассчитываем рыночные доли
        df = self.calculate_market_shares(df)
        
        # Сохраняем данные
        filepath = '/Users/tumowuh/Desktop/market analysis/real_enhanced_etf_data.csv'
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        print(f"Обогащенные данные сохранены в: {filepath}")
        
        return df
    
    def generate_real_statistics(self, df):
        """
        Генерация статистики на основе реальных данных
        """
        print("\n=== СТАТИСТИКА ПО РЕАЛЬНЫМ ДАННЫМ ===")
        
        # Общая статистика
        print(f"Всего проанализировано ETF: {len(df)}")
        
        # Качество данных
        quality_stats = df['data_quality'].value_counts()
        print(f"\nКачество собранных данных:")
        for quality, count in quality_stats.items():
            print(f"  {quality}: {count} фондов")
        
        # Статистика по УК
        uk_stats = df.groupby('management_company').agg({
            'ticker': 'count',
            'current_price': 'mean',
            'return_1y_percent': 'mean',
            'volatility_percent': 'mean',
            'uk_market_share_percent': 'first'
        }).round(2)
        
        uk_stats.columns = ['Количество ETF', 'Средняя цена', 'Средняя доходность (%)', 
                          'Средняя волатильность (%)', 'Доля рынка (%)']
        
        print(f"\nСтатистика по управляющим компаниям:")
        print(uk_stats.fillna('—'))
        
        # Лидеры по доходности (только с реальными данными)
        performance_leaders = df[df['return_1y_percent'].notna()].nlargest(5, 'return_1y_percent')
        
        if len(performance_leaders) > 0:
            print(f"\nЛидеры по доходности за год:")
            for _, row in performance_leaders.iterrows():
                print(f"  {row['ticker']} ({row['name']}): {row['return_1y_percent']:.1f}%")
        
        # Статистика по категориям
        category_stats = df.groupby('category').agg({
            'ticker': 'count',
            'return_1y_percent': 'mean',
            'volatility_percent': 'mean'
        }).round(2)
        
        print(f"\nСтатистика по категориям:")
        print(category_stats.fillna('—'))
        
        return uk_stats, category_stats

def main():
    """
    Основная функция с улучшенной обработкой ошибок
    """
    logger.info("=== ЗАПУСК СБОРА РЕАЛЬНЫХ ДАННЫХ О РОССИЙСКИХ БПИФ ===")
    
    collector = EnhancedRealETFCollector()
    
    try:
        # Создаем реальный анализ
        df = collector.create_real_market_analysis()
        
        # Генерируем статистику
        uk_stats, category_stats = collector.generate_real_statistics(df)
        
        logger.info(f"=== АНАЛИЗ УСПЕШНО ЗАВЕРШЕН ===")
        logger.info(f"Проанализировано {len(df)} ETF от {df['management_company'].nunique()} управляющих компаний")
        
        # Показываем данные с лучшим качеством
        high_quality = df[df['data_quality'] == 'Хорошее']
        if len(high_quality) > 0:
            logger.info(f"ETF с полными данными: {len(high_quality)} шт.")
            for _, etf in high_quality.iterrows():
                price = f"{etf['current_price']:.2f} руб" if pd.notna(etf['current_price']) else "—"
                perf = f"{etf['return_1y_percent']:.1f}%" if pd.notna(etf['return_1y_percent']) else "—"
                logger.info(f"  {etf['ticker']} - Цена: {price}, Доходность: {perf}")
        
        return df
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()