"""
Валидация данных для анализа российских БПИФов
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta
import warnings

from logger_config import logger

class DataValidator:
    """
    Класс для валидации и очистки данных о БПИФах
    """
    
    def __init__(self):
        self.validation_rules = {
            'price_range': (0.01, 10000),  # Разумный диапазон цен
            'return_range': (-90, 1000),   # Диапазон доходности в %
            'volatility_range': (0, 200),  # Диапазон волатильности в %
            'expense_ratio_range': (0, 10) # Диапазон комиссий в %
        }
    
    def validate_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Валидация ценовых данных
        """
        logger.info("Валидация ценовых данных")
        
        if 'current_price' not in df.columns:
            logger.warning("Столбец 'current_price' отсутствует")
            return df
        
        # Удаляем очевидно некорректные цены
        min_price, max_price = self.validation_rules['price_range']
        invalid_prices = (
            (df['current_price'] < min_price) |
            (df['current_price'] > max_price)
        ) & df['current_price'].notna()
        
        if invalid_prices.any():
            invalid_count = invalid_prices.sum()
            logger.warning(f"Найдено {invalid_count} записей с некорректными ценами")
            df.loc[invalid_prices, 'current_price'] = np.nan
        
        return df
    
    def validate_performance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Валидация данных о доходности
        """
        logger.info("Валидация данных о доходности")
        
        performance_columns = [col for col in df.columns if 'return' in col or 'performance' in col]
        
        if not performance_columns:
            logger.warning("Столбцы с данными о доходности не найдены")
            return df
        
        min_return, max_return = self.validation_rules['return_range']
        
        for col in performance_columns:
            if col in df.columns:
                invalid_returns = (
                    (df[col] < min_return) |
                    (df[col] > max_return)
                ) & df[col].notna()
                
                if invalid_returns.any():
                    invalid_count = invalid_returns.sum()
                    logger.warning(f"Найдено {invalid_count} записей с некорректной доходностью в столбце {col}")
                    df.loc[invalid_returns, col] = np.nan
        
        return df
    
    def validate_volatility_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Валидация данных о волатильности
        """
        logger.info("Валидация данных о волатильности")
        
        volatility_columns = [col for col in df.columns if 'volatility' in col]
        
        if not volatility_columns:
            logger.warning("Столбцы с данными о волатильности не найдены")
            return df
        
        min_vol, max_vol = self.validation_rules['volatility_range']
        
        for col in volatility_columns:
            if col in df.columns:
                # Волатильность не может быть отрицательной
                negative_vol = (df[col] < 0) & df[col].notna()
                if negative_vol.any():
                    logger.warning(f"Найдены отрицательные значения волатильности в столбце {col}")
                    df.loc[negative_vol, col] = np.abs(df.loc[negative_vol, col])
                
                # Проверяем на экстремальные значения
                extreme_vol = (df[col] > max_vol) & df[col].notna()
                if extreme_vol.any():
                    logger.warning(f"Найдены экстремальные значения волатильности в столбце {col}")
                    df.loc[extreme_vol, col] = np.nan
        
        return df
    
    def validate_categorical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Валидация категориальных данных
        """
        logger.info("Валидация категориальных данных")
        
        # Проверяем наличие обязательных столбцов
        required_columns = ['ticker', 'management_company', 'category']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Отсутствуют обязательные столбцы: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Проверяем на пустые тикеры
        empty_tickers = df['ticker'].isna() | (df['ticker'] == '')
        if empty_tickers.any():
            logger.error("Найдены записи с пустыми тикерами")
            df = df[~empty_tickers].copy()
        
        # Нормализуем названия управляющих компаний
        df['management_company'] = df['management_company'].str.strip()
        
        # Нормализуем категории
        df['category'] = df['category'].str.strip()
        
        return df
    
    def check_data_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Проверка полноты данных
        """
        logger.info("Проверка полноты данных")
        
        completeness_stats = {}
        
        for column in df.columns:
            non_null_count = df[column].notna().sum()
            total_count = len(df)
            completeness = (non_null_count / total_count) * 100 if total_count > 0 else 0
            completeness_stats[column] = round(completeness, 2)
        
        # Логируем столбцы с низкой полнотой данных
        low_completeness = {k: v for k, v in completeness_stats.items() if v < 50}
        if low_completeness:
            logger.warning(f"Столбцы с низкой полнотой данных (<50%): {low_completeness}")
        
        return completeness_stats
    
    def detect_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обнаружение и удаление дубликатов
        """
        logger.info("Проверка на дубликаты")
        
        # Проверяем дубликаты по тикеру
        if 'ticker' in df.columns:
            duplicate_tickers = df[df['ticker'].duplicated()]
            if not duplicate_tickers.empty:
                logger.warning(f"Найдены дубликаты тикеров: {duplicate_tickers['ticker'].tolist()}")
                df = df.drop_duplicates(subset=['ticker'], keep='first')
        
        return df
    
    def validate_data_consistency(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Проверка согласованности данных
        """
        logger.info("Проверка согласованности данных")
        
        inconsistencies = []
        
        # Проверяем соответствие доходности и волатильности
        if 'return_1y_percent' in df.columns and 'volatility_percent' in df.columns:
            # Высокая доходность при очень низкой волатильности может быть подозрительной
            suspicious = (
                (df['return_1y_percent'] > 50) &
                (df['volatility_percent'] < 5) &
                df['return_1y_percent'].notna() &
                df['volatility_percent'].notna()
            )
            
            if suspicious.any():
                suspicious_tickers = df[suspicious]['ticker'].tolist()
                inconsistencies.append({
                    'type': 'high_return_low_volatility',
                    'tickers': suspicious_tickers,
                    'description': 'Высокая доходность при низкой волатильности'
                })
        
        # Проверяем соответствие цены и категории
        if 'current_price' in df.columns and 'category' in df.columns:
            # Паи облигационных фондов обычно стоят меньше акционных
            bond_funds = df[df['category'].str.contains('облигац', case=False, na=False)]
            expensive_bonds = bond_funds[bond_funds['current_price'] > 1000]
            
            if not expensive_bonds.empty:
                inconsistencies.append({
                    'type': 'expensive_bond_funds',
                    'tickers': expensive_bonds['ticker'].tolist(),
                    'description': 'Облигационные фонды с высокой ценой пая'
                })
        
        if inconsistencies:
            logger.warning(f"Обнаружено {len(inconsistencies)} несоответствий в данных")
        
        return inconsistencies
    
    def comprehensive_validation(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Комплексная валидация данных
        """
        logger.info("Запуск комплексной валидации данных")
        
        initial_count = len(df)
        validation_report = {
            'initial_records': initial_count,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        # Последовательная валидация
        df = self.validate_categorical_data(df)
        df = self.detect_duplicates(df)
        df = self.validate_price_data(df)
        df = self.validate_performance_data(df)
        df = self.validate_volatility_data(df)
        
        # Отчеты о качестве данных
        validation_report['completeness_stats'] = self.check_data_completeness(df)
        validation_report['inconsistencies'] = self.validate_data_consistency(df)
        validation_report['final_records'] = len(df)
        validation_report['records_removed'] = initial_count - len(df)
        
        logger.info(f"Валидация завершена. Удалено {validation_report['records_removed']} записей")
        
        return df, validation_report

# Функция-помощник для быстрой валидации
def validate_etf_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Быстрая валидация DataFrame с данными о БПИФах
    """
    validator = DataValidator()
    return validator.comprehensive_validation(df)