#!/usr/bin/env python3
"""
Модуль для расчета риск-метрик ETF
Включает Sharpe Ratio, Sortino Ratio, VaR, Maximum Drawdown
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

class RiskMetricsCalculator:
    """Калькулятор риск-метрик для ETF"""
    
    def __init__(self, risk_free_rate: float = 0.15):
        """
        Инициализация калькулятора
        
        Args:
            risk_free_rate: Безрисковая ставка (ключевая ставка ЦБ РФ ~15%)
        """
        self.risk_free_rate = risk_free_rate
    
    def calculate_sharpe_ratio(self, returns: pd.Series, annual_return: float, volatility: float) -> float:
        """
        Расчет коэффициента Шарпа
        
        Args:
            returns: Временной ряд доходностей
            annual_return: Годовая доходность (%)
            volatility: Волатильность (%)
            
        Returns:
            Коэффициент Шарпа
        """
        try:
            excess_return = (annual_return / 100) - (self.risk_free_rate / 100)
            volatility_decimal = volatility / 100
            
            if volatility_decimal == 0:
                return 0.0
            
            sharpe = excess_return / volatility_decimal
            return round(sharpe, 3)
        except:
            return 0.0
    
    def calculate_sortino_ratio(self, returns: pd.Series, annual_return: float) -> float:
        """
        Расчет коэффициента Сортино (учитывает только негативную волатильность)
        
        Args:
            returns: Временной ряд доходностей
            annual_return: Годовая доходность (%)
            
        Returns:
            Коэффициент Сортино
        """
        try:
            if len(returns) < 2:
                return 0.0
            
            # Создаем синтетические дневные доходности на основе годовой
            daily_return = (annual_return / 100) / 252  # 252 торговых дня в году
            daily_returns = np.random.normal(daily_return, daily_return * 0.1, 252)
            
            # Рассчитываем downside deviation
            negative_returns = daily_returns[daily_returns < 0]
            
            if len(negative_returns) == 0:
                return float('inf')  # Нет негативных доходностей
            
            downside_deviation = np.std(negative_returns) * np.sqrt(252)  # Аннуализируем
            
            excess_return = (annual_return / 100) - (self.risk_free_rate / 100)
            
            if downside_deviation == 0:
                return float('inf')
            
            sortino = excess_return / downside_deviation
            return round(sortino, 3)
        except:
            return 0.0
    
    def calculate_maximum_drawdown(self, returns: pd.Series, current_price: float) -> float:
        """
        Расчет максимальной просадки
        
        Args:
            returns: Временной ряд доходностей
            current_price: Текущая цена
            
        Returns:
            Максимальная просадка (%)
        """
        try:
            if len(returns) < 2:
                return 0.0
            
            # Создаем синтетический ценовой ряд
            prices = [current_price]
            for i in range(251):  # Год торгов
                daily_change = np.random.normal(0, 0.02)  # 2% дневная волатильность
                new_price = prices[-1] * (1 + daily_change)
                prices.append(max(new_price, prices[-1] * 0.7))  # Ограничиваем падение
            
            prices = np.array(prices)
            
            # Рассчитываем running maximum
            running_max = np.maximum.accumulate(prices)
            
            # Рассчитываем drawdown
            drawdown = (prices - running_max) / running_max * 100
            
            max_drawdown = np.min(drawdown)
            return round(max_drawdown, 2)
        except:
            return 0.0
    
    def calculate_var(self, returns: pd.Series, annual_return: float, volatility: float, 
                     confidence_level: float = 0.05) -> float:
        """
        Расчет Value at Risk (VaR)
        
        Args:
            returns: Временной ряд доходностей
            annual_return: Годовая доходность (%)
            volatility: Волатильность (%)
            confidence_level: Уровень доверия (0.05 = 95% VaR)
            
        Returns:
            VaR (%)
        """
        try:
            # Конвертируем в дневные показатели
            daily_return = (annual_return / 100) / 252
            daily_volatility = (volatility / 100) / np.sqrt(252)
            
            # Рассчитываем VaR используя нормальное распределение
            var = stats.norm.ppf(confidence_level, daily_return, daily_volatility) * 100
            
            return round(var, 2)
        except:
            return 0.0
    
    def calculate_information_ratio(self, annual_return: float, benchmark_return: float, 
                                  volatility: float, benchmark_volatility: float) -> float:
        """
        Расчет Information Ratio (активный риск-скорректированный возврат)
        
        Args:
            annual_return: Годовая доходность ETF (%)
            benchmark_return: Доходность бенчмарка (%)
            volatility: Волатильность ETF (%)
            benchmark_volatility: Волатильность бенчмарка (%)
            
        Returns:
            Information Ratio
        """
        try:
            excess_return = (annual_return - benchmark_return) / 100
            
            # Tracking error (приблизительный расчет)
            tracking_error = np.sqrt(max(0, (volatility/100)**2 - (benchmark_volatility/100)**2))
            
            if tracking_error == 0:
                return 0.0
            
            info_ratio = excess_return / tracking_error
            return round(info_ratio, 3)
        except:
            return 0.0
    
    def calculate_beta(self, annual_return: float, volatility: float, 
                      market_return: float = 12.0, market_volatility: float = 20.0) -> float:
        """
        Расчет бета-коэффициента относительно рынка
        
        Args:
            annual_return: Годовая доходность ETF (%)
            volatility: Волатильность ETF (%)
            market_return: Доходность рынка (по умолчанию 12%)
            market_volatility: Волатильность рынка (по умолчанию 20%)
            
        Returns:
            Бета-коэффициент
        """
        try:
            # Упрощенный расчет бета через корреляцию и волатильности
            # Предполагаем корреляцию 0.7 для российских ETF с рынком
            correlation = 0.7
            
            beta = correlation * (volatility / market_volatility)
            return round(beta, 3)
        except:
            return 1.0
    
    def calculate_alpha(self, annual_return: float, beta: float, 
                       market_return: float = 12.0) -> float:
        """
        Расчет альфа-коэффициента (избыточная доходность)
        
        Args:
            annual_return: Годовая доходность ETF (%)
            beta: Бета-коэффициент
            market_return: Доходность рынка (по умолчанию 12%)
            
        Returns:
            Альфа-коэффициент (%)
        """
        try:
            expected_return = self.risk_free_rate + beta * (market_return - self.risk_free_rate)
            alpha = annual_return - expected_return
            return round(alpha, 2)
        except:
            return 0.0
    
    def calculate_all_metrics(self, etf_data: Dict) -> Dict:
        """
        Расчет всех риск-метрик для ETF
        
        Args:
            etf_data: Словарь с данными ETF
            
        Returns:
            Словарь со всеми рассчитанными метриками
        """
        try:
            annual_return = etf_data.get('annual_return', 0)
            volatility = etf_data.get('volatility', 0)
            current_price = etf_data.get('current_price', 100)
            
            # Создаем синтетический ряд доходностей для расчетов
            returns = pd.Series(np.random.normal(annual_return/252/100, volatility/252/100, 252))
            
            # Рассчитываем бета и альфа
            beta = self.calculate_beta(annual_return, volatility)
            alpha = self.calculate_alpha(annual_return, beta)
            
            metrics = {
                'sharpe_ratio': self.calculate_sharpe_ratio(returns, annual_return, volatility),
                'sortino_ratio': self.calculate_sortino_ratio(returns, annual_return),
                'max_drawdown': self.calculate_maximum_drawdown(returns, current_price),
                'var_95': self.calculate_var(returns, annual_return, volatility),
                'information_ratio': self.calculate_information_ratio(annual_return, 12.0, volatility, 20.0),
                'beta': beta,
                'alpha': alpha,
                'risk_adjusted_return': annual_return / max(volatility, 1) * 100,  # Простая риск-скорректированная доходность
            }
            
            return metrics
            
        except Exception as e:
            print(f"Ошибка расчета метрик: {e}")
            return {
                'sharpe_ratio': 0.0,
                'sortino_ratio': 0.0,
                'max_drawdown': 0.0,
                'var_95': 0.0,
                'information_ratio': 0.0,
                'beta': 1.0,
                'alpha': 0.0,
                'risk_adjusted_return': 0.0
            }

def calculate_portfolio_metrics(etf_list: List[Dict], weights: Optional[List[float]] = None) -> Dict:
    """
    Расчет метрик для портфеля ETF
    
    Args:
        etf_list: Список ETF с данными
        weights: Веса ETF в портфеле (если None, то равновесный)
        
    Returns:
        Метрики портфеля
    """
    if not etf_list:
        return {}
    
    n = len(etf_list)
    if weights is None:
        weights = [1/n] * n
    
    # Портфельная доходность
    portfolio_return = sum(w * etf.get('annual_return', 0) for w, etf in zip(weights, etf_list))
    
    # Упрощенный расчет портфельной волатильности (без корреляций)
    portfolio_volatility = np.sqrt(sum((w * etf.get('volatility', 0))**2 for w, etf in zip(weights, etf_list)))
    
    calculator = RiskMetricsCalculator()
    
    return {
        'portfolio_return': round(portfolio_return, 2),
        'portfolio_volatility': round(portfolio_volatility, 2),
        'portfolio_sharpe': calculator.calculate_sharpe_ratio(pd.Series(), portfolio_return, portfolio_volatility),
        'diversification_ratio': round(sum(w * etf.get('volatility', 0) for w, etf in zip(weights, etf_list)) / portfolio_volatility, 2)
    }