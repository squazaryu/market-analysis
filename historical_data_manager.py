#!/usr/bin/env python3
"""
Менеджер исторических данных для анализа рынка ETF
Управляет загрузкой, кэшированием и обработкой исторических данных
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path
import logging

class HistoricalDataManager:
    """Менеджер для работы с историческими данными ETF"""
    
    def __init__(self, cache_dir: str = "./cache"):
        """
        Инициализация менеджера исторических данных
        
        Args:
            cache_dir: Директория для кэширования данных
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.logger = self._setup_logger()
        self.historical_data: Dict[str, pd.DataFrame] = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('HistoricalDataManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def load_historical_data(self, ticker: str, start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Загружает исторические данные для указанного тикера
        
        Args:
            ticker: Тикер ETF
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            DataFrame с историческими данными
        """
        try:
            # Проверяем кэш
            cache_file = self.cache_dir / f"{ticker}_historical.csv"
            
            if cache_file.exists():
                data = pd.read_csv(cache_file, parse_dates=['date'])
                self.logger.info(f"Загружены данные для {ticker} из кэша")
                
                # Фильтруем по датам если указаны
                if start_date:
                    data = data[data['date'] >= start_date]
                if end_date:
                    data = data[data['date'] <= end_date]
                    
                return data
            else:
                # Если нет в кэше, создаем заглушку с синтетическими данными
                self.logger.warning(f"Нет исторических данных для {ticker}, создаем синтетические данные")
                return self._generate_synthetic_data(ticker, start_date, end_date)
                
        except Exception as e:
            self.logger.error(f"Ошибка загрузки данных для {ticker}: {e}")
            return pd.DataFrame()
    
    def _generate_synthetic_data(self, ticker: str, start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Генерирует синтетические исторические данные для демонстрации
        
        Args:
            ticker: Тикер ETF
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            DataFrame с синтетическими данными
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now()
            
        # Создаем диапазон дат
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        dates = dates[dates.weekday < 5]  # Только рабочие дни
        
        # Генерируем цены с некоторой случайностью
        np.random.seed(hash(ticker) % 2**32)  # Воспроизводимая случайность на основе тикера
        
        initial_price = 100 + np.random.uniform(-20, 20)
        returns = np.random.normal(0.0005, 0.02, len(dates))  # Ежедневная доходность
        
        prices = [initial_price]
        for ret in returns[1:]:
            prices.append(prices[-1] * (1 + ret))
        
        # Создаем DataFrame
        data = pd.DataFrame({
            'date': dates,
            'ticker': ticker,
            'close': prices,
            'volume': np.random.randint(1000, 100000, len(dates)),
            'open': [p * (1 + np.random.uniform(-0.01, 0.01)) for p in prices],
            'high': [p * (1 + np.random.uniform(0, 0.02)) for p in prices],
            'low': [p * (1 + np.random.uniform(-0.02, 0)) for p in prices],
        })
        
        return data
    
    def get_price_history(self, ticker: str, days: int = 252) -> pd.DataFrame:
        """
        Получает историю цен для указанного периода
        
        Args:
            ticker: Тикер ETF
            days: Количество торговых дней
            
        Returns:
            DataFrame с историей цен
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=int(days * 1.4))  # Добавляем запас на выходные
        
        return self.load_historical_data(ticker, start_date, end_date)
    
    def calculate_returns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает доходности
        
        Args:
            data: DataFrame с ценами
            
        Returns:
            DataFrame с доходностями
        """
        if 'close' not in data.columns:
            return data
            
        data = data.copy()
        data['daily_return'] = data['close'].pct_change()
        data['cumulative_return'] = (1 + data['daily_return']).cumprod() - 1
        
        return data
    
    def calculate_volatility(self, data: pd.DataFrame, window: int = 20) -> pd.DataFrame:
        """
        Рассчитывает волатильность
        
        Args:
            data: DataFrame с доходностями
            window: Окно для расчета скользящей волатильности
            
        Returns:
            DataFrame с волатильностью
        """
        if 'daily_return' not in data.columns:
            data = self.calculate_returns(data)
            
        data = data.copy()
        data['volatility'] = data['daily_return'].rolling(window=window).std() * np.sqrt(252)
        
        return data
    
    def get_performance_metrics(self, ticker: str, days: int = 252) -> Dict[str, float]:
        """
        Рассчитывает метрики производительности
        
        Args:
            ticker: Тикер ETF
            days: Период для расчета
            
        Returns:
            Словарь с метриками
        """
        try:
            data = self.get_price_history(ticker, days)
            
            if data.empty:
                return {}
                
            data = self.calculate_returns(data)
            
            if 'daily_return' not in data.columns or data['daily_return'].isna().all():
                return {}
            
            # Рассчитываем метрики
            total_return = data['cumulative_return'].iloc[-1] if not data['cumulative_return'].isna().all() else 0
            volatility = data['daily_return'].std() * np.sqrt(252) if not data['daily_return'].isna().all() else 0
            sharpe_ratio = (data['daily_return'].mean() * 252) / volatility if volatility > 0 else 0
            
            max_drawdown = 0
            if not data['cumulative_return'].isna().all():
                cumulative = (1 + data['daily_return']).cumprod()
                running_max = cumulative.expanding().max()
                drawdown = (cumulative - running_max) / running_max
                max_drawdown = drawdown.min()
            
            return {
                'total_return': round(total_return * 100, 2),
                'annualized_volatility': round(volatility * 100, 2),
                'sharpe_ratio': round(sharpe_ratio, 2),
                'max_drawdown': round(max_drawdown * 100, 2),
                'trading_days': len(data)
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка расчета метрик для {ticker}: {e}")
            return {}
    
    def save_to_cache(self, ticker: str, data: pd.DataFrame):
        """
        Сохраняет данные в кэш
        
        Args:
            ticker: Тикер ETF
            data: Данные для сохранения
        """
        try:
            cache_file = self.cache_dir / f"{ticker}_historical.csv"
            data.to_csv(cache_file, index=False)
            self.logger.info(f"Данные для {ticker} сохранены в кэш")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения в кэш для {ticker}: {e}")
    
    def get_correlation_matrix(self, tickers: List[str], days: int = 252) -> pd.DataFrame:
        """
        Рассчитывает корреляционную матрицу для списка тикеров
        
        Args:
            tickers: Список тикеров
            days: Период для расчета
            
        Returns:
            Корреляционная матрица
        """
        returns_data = {}
        
        for ticker in tickers:
            try:
                data = self.get_price_history(ticker, days)
                if not data.empty:
                    data = self.calculate_returns(data)
                    if 'daily_return' in data.columns:
                        returns_data[ticker] = data.set_index('date')['daily_return']
            except Exception as e:
                self.logger.warning(f"Не удалось загрузить данные для {ticker}: {e}")
        
        if not returns_data:
            return pd.DataFrame()
            
        # Создаем DataFrame с доходностями
        returns_df = pd.DataFrame(returns_data)
        
        # Рассчитываем корреляцию
        correlation_matrix = returns_df.corr()
        
        return correlation_matrix
    
    def clear_cache(self):
        """Очищает кэш исторических данных"""
        try:
            for cache_file in self.cache_dir.glob("*_historical.csv"):
                cache_file.unlink()
            self.logger.info("Кэш очищен")
        except Exception as e:
            self.logger.error(f"Ошибка очистки кэша: {e}")

def main():
    """Тестирование менеджера исторических данных"""
    
    manager = HistoricalDataManager()
    
    # Тестовые тикеры
    test_tickers = ['TMOS', 'SBGB', 'RUSE']
    
    print("🔄 Тестирование HistoricalDataManager...")
    
    for ticker in test_tickers:
        print(f"\n📊 Тестирование {ticker}:")
        
        # Загружаем данные
        data = manager.get_price_history(ticker, 100)
        print(f"   Загружено {len(data)} записей")
        
        # Рассчитываем метрики
        metrics = manager.get_performance_metrics(ticker, 100)
        if metrics:
            print(f"   Доходность: {metrics.get('total_return', 0)}%")
            print(f"   Волатильность: {metrics.get('annualized_volatility', 0)}%")
            print(f"   Sharpe: {metrics.get('sharpe_ratio', 0)}")
    
    # Тестируем корреляционную матрицу
    print(f"\n📈 Корреляционная матрица:")
    corr_matrix = manager.get_correlation_matrix(test_tickers, 100)
    if not corr_matrix.empty:
        print(corr_matrix.round(2))
    
    print(f"\n✅ Тестирование завершено!")

if __name__ == "__main__":
    main()