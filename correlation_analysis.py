#!/usr/bin/env python3
"""
Модуль для корреляционного анализа ETF
Включает корреляционные матрицы, кластерный анализ, диверсификационные возможности
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from scipy.spatial.distance import pdist, squareform
import warnings
warnings.filterwarnings('ignore')

class CorrelationAnalyzer:
    """Анализатор корреляций между ETF"""
    
    def __init__(self):
        self.correlation_matrix = None
        self.clusters = None
    
    def generate_synthetic_correlations(self, etf_data: List[Dict]) -> pd.DataFrame:
        """
        Генерирует синтетическую корреляционную матрицу на основе характеристик ETF
        
        Args:
            etf_data: Список данных ETF
            
        Returns:
            Корреляционная матрица
        """
        tickers = [etf['ticker'] for etf in etf_data]
        n = len(tickers)
        
        # Создаем базовую корреляционную матрицу
        correlation_matrix = np.eye(n)
        
        for i in range(n):
            for j in range(i+1, n):
                etf1, etf2 = etf_data[i], etf_data[j]
                
                # Рассчитываем корреляцию на основе характеристик
                correlation = self._calculate_synthetic_correlation(etf1, etf2)
                correlation_matrix[i, j] = correlation
                correlation_matrix[j, i] = correlation
        
        return pd.DataFrame(correlation_matrix, index=tickers, columns=tickers)
    
    def _calculate_synthetic_correlation(self, etf1: Dict, etf2: Dict) -> float:
        """
        Рассчитывает синтетическую корреляцию между двумя ETF
        на основе их характеристик
        """
        # Базовая корреляция для российского рынка
        base_correlation = 0.6
        
        # Факторы, влияющие на корреляцию
        factors = []
        
        # 1. Схожесть доходности
        return_diff = abs(etf1.get('annual_return', 0) - etf2.get('annual_return', 0))
        return_factor = max(0, 1 - return_diff / 50)  # Чем ближе доходности, тем выше корреляция
        factors.append(return_factor * 0.3)
        
        # 2. Схожесть волатильности
        vol_diff = abs(etf1.get('volatility', 0) - etf2.get('volatility', 0))
        vol_factor = max(0, 1 - vol_diff / 30)
        factors.append(vol_factor * 0.2)
        
        # 3. Схожесть по управляющей компании
        same_manager = etf1.get('management_company', '') == etf2.get('management_company', '')
        if same_manager and etf1.get('management_company', '') != '':
            factors.append(0.15)
        
        # 4. Схожесть названий (примерная категоризация)
        name1 = etf1.get('short_name', '').lower()
        name2 = etf2.get('short_name', '').lower()
        
        # Облигационные ETF
        if any(word in name1 for word in ['bond', 'облиг', 'gb', 'cb']) and \
           any(word in name2 for word in ['bond', 'облиг', 'gb', 'cb']):
            factors.append(0.4)
        
        # Акционные ETF
        elif any(word in name1 for word in ['equity', 'акц', 'mx', 'index']) and \
             any(word in name2 for word in ['equity', 'акц', 'mx', 'index']):
            factors.append(0.35)
        
        # Золото и сырье
        elif any(word in name1 for word in ['gold', 'золот', 'gld']) and \
             any(word in name2 for word in ['gold', 'золот', 'gld']):
            factors.append(0.3)
        
        # Валютные ETF
        elif any(word in name1 for word in ['usd', 'eur', 'yuan', 'валют']) and \
             any(word in name2 for word in ['usd', 'eur', 'yuan', 'валют']):
            factors.append(0.25)
        
        # Рассчитываем итоговую корреляцию
        total_factor = sum(factors)
        correlation = base_correlation + total_factor - 0.3  # Корректировка
        
        # Добавляем случайность
        noise = np.random.normal(0, 0.1)
        correlation += noise
        
        # Ограничиваем диапазон
        correlation = max(-0.5, min(0.95, correlation))
        
        return round(correlation, 3)
    
    def perform_cluster_analysis(self, correlation_matrix: pd.DataFrame, 
                                n_clusters: int = 5) -> Dict:
        """
        Выполняет кластерный анализ ETF на основе корреляций
        
        Args:
            correlation_matrix: Корреляционная матрица
            n_clusters: Количество кластеров
            
        Returns:
            Результаты кластеризации
        """
        # Преобразуем корреляции в расстояния
        distance_matrix = 1 - correlation_matrix.abs()
        
        # Иерархическая кластеризация
        condensed_distances = pdist(distance_matrix.values)
        linkage_matrix = linkage(condensed_distances, method='ward')
        
        # Получаем кластеры
        cluster_labels = fcluster(linkage_matrix, n_clusters, criterion='maxclust')
        
        # Группируем ETF по кластерам
        clusters = {}
        for i, ticker in enumerate(correlation_matrix.index):
            cluster_id = cluster_labels[i]
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(ticker)
        
        return {
            'clusters': clusters,
            'linkage_matrix': linkage_matrix,
            'cluster_labels': cluster_labels,
            'tickers': list(correlation_matrix.index)
        }
    
    def find_diversification_opportunities(self, correlation_matrix: pd.DataFrame, 
                                         etf_data: List[Dict], 
                                         threshold: float = 0.3) -> List[Dict]:
        """
        Находит возможности для диверсификации (низкокоррелированные пары)
        
        Args:
            correlation_matrix: Корреляционная матрица
            etf_data: Данные ETF
            threshold: Порог корреляции для диверсификации
            
        Returns:
            Список диверсификационных возможностей
        """
        opportunities = []
        etf_dict = {etf['ticker']: etf for etf in etf_data}
        
        # Находим пары с низкой корреляцией
        for i in range(len(correlation_matrix.index)):
            for j in range(i+1, len(correlation_matrix.columns)):
                ticker1 = correlation_matrix.index[i]
                ticker2 = correlation_matrix.columns[j]
                correlation = correlation_matrix.iloc[i, j]
                
                if abs(correlation) < threshold:
                    etf1 = etf_dict.get(ticker1, {})
                    etf2 = etf_dict.get(ticker2, {})
                    
                    # Рассчитываем потенциальную выгоду от диверсификации
                    return1 = etf1.get('annual_return', 0)
                    return2 = etf2.get('annual_return', 0)
                    vol1 = etf1.get('volatility', 0)
                    vol2 = etf2.get('volatility', 0)
                    
                    # Портфельные характеристики (50/50)
                    portfolio_return = (return1 + return2) / 2
                    portfolio_vol = np.sqrt((vol1**2 + vol2**2 + 2*correlation*vol1*vol2) / 4)
                    
                    diversification_benefit = ((vol1 + vol2) / 2) - portfolio_vol
                    
                    opportunities.append({
                        'etf1': ticker1,
                        'etf2': ticker2,
                        'correlation': correlation,
                        'portfolio_return': round(portfolio_return, 2),
                        'portfolio_volatility': round(portfolio_vol, 2),
                        'diversification_benefit': round(diversification_benefit, 2),
                        'sharpe_improvement': round(portfolio_return / max(portfolio_vol, 1), 3)
                    })
        
        # Сортируем по выгоде от диверсификации
        opportunities.sort(key=lambda x: x['diversification_benefit'], reverse=True)
        
        return opportunities[:10]  # Топ-10 возможностей
    
    def calculate_portfolio_correlation_risk(self, tickers: List[str], 
                                           weights: List[float],
                                           correlation_matrix: pd.DataFrame) -> Dict:
        """
        Рассчитывает корреляционный риск портфеля
        
        Args:
            tickers: Список тикеров в портфеле
            weights: Веса активов
            correlation_matrix: Корреляционная матрица
            
        Returns:
            Метрики корреляционного риска
        """
        if len(tickers) != len(weights):
            return {}
        
        # Извлекаем подматрицу для портфеля
        portfolio_corr = correlation_matrix.loc[tickers, tickers]
        weights_array = np.array(weights)
        
        # Средняя корреляция в портфеле
        n = len(tickers)
        total_correlation = 0
        count = 0
        
        for i in range(n):
            for j in range(i+1, n):
                correlation = portfolio_corr.iloc[i, j]
                weight_product = weights[i] * weights[j]
                total_correlation += correlation * weight_product
                count += 1
        
        avg_correlation = total_correlation / max(count, 1) if count > 0 else 0
        
        # Эффективное количество активов (Diversification Ratio)
        sum_weights_squared = sum(w**2 for w in weights)
        effective_assets = 1 / sum_weights_squared
        
        # Концентрационный риск
        concentration_risk = max(weights) - (1/n)  # Отклонение от равновесного портфеля
        
        return {
            'average_correlation': round(avg_correlation, 3),
            'effective_number_of_assets': round(effective_assets, 2),
            'concentration_risk': round(concentration_risk, 3),
            'diversification_score': round((1 - avg_correlation) * effective_assets / n, 3)
        }
    
    def get_correlation_insights(self, correlation_matrix: pd.DataFrame, 
                               etf_data: List[Dict]) -> Dict:
        """
        Получает инсайты из корреляционного анализа
        
        Args:
            correlation_matrix: Корреляционная матрица
            etf_data: Данные ETF
            
        Returns:
            Словарь с инсайтами
        """
        etf_dict = {etf['ticker']: etf for etf in etf_data}
        
        # Находим самые коррелированные пары
        high_corr_pairs = []
        low_corr_pairs = []
        
        for i in range(len(correlation_matrix.index)):
            for j in range(i+1, len(correlation_matrix.columns)):
                ticker1 = correlation_matrix.index[i]
                ticker2 = correlation_matrix.columns[j]
                correlation = correlation_matrix.iloc[i, j]
                
                pair_info = {
                    'etf1': ticker1,
                    'etf2': ticker2,
                    'correlation': correlation,
                    'etf1_return': etf_dict.get(ticker1, {}).get('annual_return', 0),
                    'etf2_return': etf_dict.get(ticker2, {}).get('annual_return', 0)
                }
                
                if correlation > 0.7:
                    high_corr_pairs.append(pair_info)
                elif correlation < 0.2:
                    low_corr_pairs.append(pair_info)
        
        # Сортируем
        high_corr_pairs.sort(key=lambda x: x['correlation'], reverse=True)
        low_corr_pairs.sort(key=lambda x: x['correlation'])
        
        # Средняя корреляция по рынку
        correlations = []
        for i in range(len(correlation_matrix.index)):
            for j in range(i+1, len(correlation_matrix.columns)):
                correlations.append(correlation_matrix.iloc[i, j])
        
        market_avg_correlation = np.mean(correlations)
        
        return {
            'market_average_correlation': round(market_avg_correlation, 3),
            'highest_correlations': high_corr_pairs[:5],
            'lowest_correlations': low_corr_pairs[:5],
            'total_pairs_analyzed': len(correlations),
            'high_correlation_pairs_count': len(high_corr_pairs),
            'diversification_opportunities_count': len(low_corr_pairs)
        }