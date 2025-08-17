#!/usr/bin/env python3
"""
Модуль для оптимизации портфеля ETF
Включает построение эффективной границы, поиск оптимальных портфелей
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy.optimize import minimize
import warnings
warnings.filterwarnings('ignore')

class PortfolioOptimizer:
    """Оптимизатор портфеля ETF"""
    
    def __init__(self, risk_free_rate: float = 0.15):
        """
        Инициализация оптимизатора
        
        Args:
            risk_free_rate: Безрисковая ставка (%)
        """
        self.risk_free_rate = risk_free_rate / 100  # Конвертируем в десятичную дробь
    
    def prepare_data(self, etf_data: List[Dict]) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """
        Подготавливает данные для оптимизации
        
        Args:
            etf_data: Список данных ETF
            
        Returns:
            Кортеж (доходности, ковариационная матрица, тикеры)
        """
        tickers = [etf['ticker'] for etf in etf_data]
        returns = np.array([etf.get('annual_return', 0) / 100 for etf in etf_data])
        volatilities = np.array([etf.get('volatility', 0) / 100 for etf in etf_data])
        
        # Создаем упрощенную ковариационную матрицу
        # В реальности нужны исторические данные, здесь используем приближение
        n = len(etf_data)
        cov_matrix = np.zeros((n, n))
        
        # Диагональные элементы - дисперсии
        np.fill_diagonal(cov_matrix, volatilities**2)
        
        # Недиагональные элементы - ковариации (упрощенный подход)
        for i in range(n):
            for j in range(i+1, n):
                # Предполагаем корреляцию на основе типа активов
                correlation = self._estimate_correlation(etf_data[i], etf_data[j])
                covariance = correlation * volatilities[i] * volatilities[j]
                cov_matrix[i, j] = covariance
                cov_matrix[j, i] = covariance
        
        return returns, cov_matrix, tickers
    
    def _estimate_correlation(self, etf1: Dict, etf2: Dict) -> float:
        """Оценивает корреляцию между двумя ETF"""
        # Базовая корреляция для российского рынка
        base_corr = 0.5
        
        name1 = etf1.get('short_name', '').lower()
        name2 = etf2.get('short_name', '').lower()
        
        # Облигационные ETF
        if any(word in name1 for word in ['bond', 'облиг', 'gb', 'cb']) and \
           any(word in name2 for word in ['bond', 'облиг', 'gb', 'cb']):
            return 0.7
        
        # Акционные ETF
        elif any(word in name1 for word in ['equity', 'акц', 'mx']) and \
             any(word in name2 for word in ['equity', 'акц', 'mx']):
            return 0.8
        
        # Золото
        elif any(word in name1 for word in ['gold', 'золот']) and \
             any(word in name2 for word in ['gold', 'золот']):
            return 0.6
        
        # Валютные
        elif any(word in name1 for word in ['usd', 'eur', 'yuan']) and \
             any(word in name2 for word in ['usd', 'eur', 'yuan']):
            return 0.4
        
        # Разные типы активов
        else:
            return 0.2
    
    def calculate_portfolio_performance(self, weights: np.ndarray, 
                                      returns: np.ndarray, 
                                      cov_matrix: np.ndarray) -> Tuple[float, float]:
        """
        Рассчитывает доходность и риск портфеля
        
        Args:
            weights: Веса активов
            returns: Ожидаемые доходности
            cov_matrix: Ковариационная матрица
            
        Returns:
            Кортеж (доходность, волатильность)
        """
        portfolio_return = np.sum(weights * returns)
        portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        portfolio_volatility = np.sqrt(portfolio_variance)
        
        return portfolio_return, portfolio_volatility
    
    def minimize_volatility(self, returns: np.ndarray, cov_matrix: np.ndarray, 
                           target_return: float) -> np.ndarray:
        """
        Находит портфель с минимальным риском для заданной доходности
        
        Args:
            returns: Ожидаемые доходности
            cov_matrix: Ковариационная матрица
            target_return: Целевая доходность
            
        Returns:
            Оптимальные веса
        """
        n = len(returns)
        
        # Целевая функция - минимизация дисперсии
        def objective(weights):
            return np.dot(weights.T, np.dot(cov_matrix, weights))
        
        # Ограничения
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},  # Сумма весов = 1
            {'type': 'eq', 'fun': lambda x: np.sum(x * returns) - target_return}  # Целевая доходность
        ]
        
        # Границы весов (от 0 до 0.4 для диверсификации)
        bounds = tuple((0, 0.4) for _ in range(n))
        
        # Начальные веса (равновесный портфель)
        initial_weights = np.array([1/n] * n)
        
        # Оптимизация
        result = minimize(objective, initial_weights, method='SLSQP', 
                         bounds=bounds, constraints=constraints)
        
        if result.success:
            return result.x
        else:
            return initial_weights
    
    def maximize_sharpe_ratio(self, returns: np.ndarray, cov_matrix: np.ndarray) -> np.ndarray:
        """
        Находит портфель с максимальным коэффициентом Шарпа
        
        Args:
            returns: Ожидаемые доходности
            cov_matrix: Ковариационная матрица
            
        Returns:
            Оптимальные веса
        """
        n = len(returns)
        
        # Целевая функция - минимизация отрицательного коэффициента Шарпа
        def objective(weights):
            portfolio_return, portfolio_volatility = self.calculate_portfolio_performance(
                weights, returns, cov_matrix)
            
            if portfolio_volatility == 0:
                return -np.inf
            
            sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_volatility
            return -sharpe_ratio  # Минимизируем отрицательное значение
        
        # Ограничения
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}  # Сумма весов = 1
        ]
        
        # Границы весов
        bounds = tuple((0, 0.5) for _ in range(n))
        
        # Начальные веса
        initial_weights = np.array([1/n] * n)
        
        # Оптимизация
        result = minimize(objective, initial_weights, method='SLSQP', 
                         bounds=bounds, constraints=constraints)
        
        if result.success:
            return result.x
        else:
            return initial_weights
    
    def build_efficient_frontier(self, returns: np.ndarray, cov_matrix: np.ndarray, 
                                num_portfolios: int = 50) -> Dict:
        """
        Строит эффективную границу
        
        Args:
            returns: Ожидаемые доходности
            cov_matrix: Ковариационная матрица
            num_portfolios: Количество портфелей на границе
            
        Returns:
            Словарь с данными эффективной границы
        """
        # Диапазон целевых доходностей
        min_return = np.min(returns)
        max_return = np.max(returns)
        target_returns = np.linspace(min_return, max_return, num_portfolios)
        
        efficient_portfolios = []
        
        for target_return in target_returns:
            try:
                weights = self.minimize_volatility(returns, cov_matrix, target_return)
                portfolio_return, portfolio_volatility = self.calculate_portfolio_performance(
                    weights, returns, cov_matrix)
                
                sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_volatility
                
                efficient_portfolios.append({
                    'return': portfolio_return,
                    'volatility': portfolio_volatility,
                    'sharpe_ratio': sharpe_ratio,
                    'weights': weights.tolist()
                })
            except:
                continue
        
        return {
            'portfolios': efficient_portfolios,
            'returns': [p['return'] for p in efficient_portfolios],
            'volatilities': [p['volatility'] for p in efficient_portfolios],
            'sharpe_ratios': [p['sharpe_ratio'] for p in efficient_portfolios]
        }
    
    def find_optimal_portfolios(self, etf_data: List[Dict]) -> Dict:
        """
        Находит различные оптимальные портфели
        
        Args:
            etf_data: Данные ETF
            
        Returns:
            Словарь с оптимальными портфелями
        """
        returns, cov_matrix, tickers = self.prepare_data(etf_data)
        
        # 1. Портфель минимального риска
        min_var_weights = self.minimize_volatility(returns, cov_matrix, np.mean(returns))
        min_var_return, min_var_vol = self.calculate_portfolio_performance(
            min_var_weights, returns, cov_matrix)
        
        # 2. Портфель максимального Шарпа
        max_sharpe_weights = self.maximize_sharpe_ratio(returns, cov_matrix)
        max_sharpe_return, max_sharpe_vol = self.calculate_portfolio_performance(
            max_sharpe_weights, returns, cov_matrix)
        
        # 3. Равновесный портфель
        equal_weights = np.array([1/len(tickers)] * len(tickers))
        equal_return, equal_vol = self.calculate_portfolio_performance(
            equal_weights, returns, cov_matrix)
        
        # 4. Портфель максимальной доходности (топ-5 ETF)
        top_performers_idx = np.argsort(returns)[-5:]
        max_return_weights = np.zeros(len(tickers))
        max_return_weights[top_performers_idx] = 0.2  # Равные веса в топ-5
        max_return_return, max_return_vol = self.calculate_portfolio_performance(
            max_return_weights, returns, cov_matrix)
        
        return {
            'minimum_variance': {
                'weights': dict(zip(tickers, min_var_weights)),
                'return': min_var_return * 100,
                'volatility': min_var_vol * 100,
                'sharpe_ratio': (min_var_return - self.risk_free_rate) / min_var_vol,
                'description': 'Портфель с минимальным риском'
            },
            'maximum_sharpe': {
                'weights': dict(zip(tickers, max_sharpe_weights)),
                'return': max_sharpe_return * 100,
                'volatility': max_sharpe_vol * 100,
                'sharpe_ratio': (max_sharpe_return - self.risk_free_rate) / max_sharpe_vol,
                'description': 'Портфель с максимальным коэффициентом Шарпа'
            },
            'equal_weighted': {
                'weights': dict(zip(tickers, equal_weights)),
                'return': equal_return * 100,
                'volatility': equal_vol * 100,
                'sharpe_ratio': (equal_return - self.risk_free_rate) / equal_vol,
                'description': 'Равновесный портфель'
            },
            'high_return': {
                'weights': dict(zip(tickers, max_return_weights)),
                'return': max_return_return * 100,
                'volatility': max_return_vol * 100,
                'sharpe_ratio': (max_return_return - self.risk_free_rate) / max_return_vol,
                'description': 'Портфель высокой доходности (топ-5 ETF)'
            }
        }
    
    def calculate_diversification_metrics(self, weights: np.ndarray, 
                                        volatilities: np.ndarray,
                                        portfolio_volatility: float) -> Dict:
        """
        Рассчитывает метрики диверсификации
        
        Args:
            weights: Веса портфеля
            volatilities: Волатильности активов
            portfolio_volatility: Волатильность портфеля
            
        Returns:
            Метрики диверсификации
        """
        # Взвешенная средняя волатильность
        weighted_avg_vol = np.sum(weights * volatilities)
        
        # Коэффициент диверсификации
        diversification_ratio = weighted_avg_vol / portfolio_volatility
        
        # Эффективное количество активов
        effective_assets = 1 / np.sum(weights**2)
        
        # Индекс концентрации Херфиндаля
        herfindahl_index = np.sum(weights**2)
        
        return {
            'diversification_ratio': round(diversification_ratio, 3),
            'effective_number_of_assets': round(effective_assets, 2),
            'herfindahl_index': round(herfindahl_index, 3),
            'concentration_score': round(np.max(weights), 3)
        }