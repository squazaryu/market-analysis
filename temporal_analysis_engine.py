#!/usr/bin/env python3
"""
Движок временного анализа для БПИФ
Обеспечивает детальные временные фильтры и анализ изменений показателей
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import json
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import logging

class TimeFrame(Enum):
    """Временные интервалы для анализа"""
    DAILY = "1D"
    WEEKLY = "1W"
    MONTHLY = "1M"
    QUARTERLY = "3M"
    SEMI_ANNUAL = "6M"
    ANNUAL = "1Y"
    TWO_YEARS = "2Y"
    THREE_YEARS = "3Y"
    FIVE_YEARS = "5Y"

class MarketPeriod(Enum):
    """Рыночные периоды"""
    CRISIS_2020 = ("2020-02-01", "2020-06-01", "Пандемия COVID-19")
    RECOVERY_2020 = ("2020-06-01", "2021-01-01", "Восстановление 2020")
    BULL_2021 = ("2021-01-01", "2022-02-24", "Бычий рынок 2021")
    SANCTIONS_2022 = ("2022-02-24", "2022-12-31", "Санкции и СВО")
    STABILIZATION_2023 = ("2023-01-01", "2024-01-01", "Стабилизация 2023")
    CURRENT_2024 = ("2024-01-01", None, "Текущий период 2024")

@dataclass
class TemporalFilter:
    """Настройки временного фильтра"""
    start_date: datetime
    end_date: datetime
    timeframe: TimeFrame
    comparison_period: Optional[Tuple[datetime, datetime]] = None
    benchmark_tickers: List[str] = None
    sectors_filter: List[str] = None
    exclude_weekends: bool = True
    include_dividends: bool = True

class TemporalAnalysisEngine:
    """Движок для временного анализа БПИФ"""
    
    def __init__(self, etf_data: pd.DataFrame, historical_manager=None):
        self.etf_data = etf_data
        self.historical_manager = historical_manager
        self.logger = self._setup_logger()
        self._cache = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('TemporalAnalysisEngine')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def create_filter(self, start_date: Union[str, datetime], end_date: Union[str, datetime] = None,
                     timeframe: TimeFrame = TimeFrame.DAILY, **kwargs) -> TemporalFilter:
        """
        Создает временной фильтр
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата (если None, то текущая дата)
            timeframe: Временной интервал
            **kwargs: Дополнительные параметры фильтра
            
        Returns:
            Объект TemporalFilter
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        
        if end_date is None:
            end_date = datetime.now()
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            
        return TemporalFilter(
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
            **kwargs
        )
    
    def get_market_period_filter(self, period: MarketPeriod) -> TemporalFilter:
        """
        Создает фильтр для конкретного рыночного периода
        
        Args:
            period: Рыночный период
            
        Returns:
            Временной фильтр для указанного периода
        """
        start_str, end_str, description = period.value
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        
        if end_str is None:
            end_date = datetime.now()
        else:
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            
        return TemporalFilter(
            start_date=start_date,
            end_date=end_date,
            timeframe=TimeFrame.DAILY
        )
    
    def apply_temporal_filter(self, data: pd.DataFrame, temp_filter: TemporalFilter) -> pd.DataFrame:
        """
        Применяет временной фильтр к данным
        
        Args:
            data: Исходные данные
            temp_filter: Временной фильтр
            
        Returns:
            Отфильтрованные данные
        """
        filtered_data = data.copy()
        
        # Фильтрация по секторам если указано
        if temp_filter.sectors_filter:
            # Добавляем информацию о секторах если её нет
            if 'sector' not in filtered_data.columns:
                filtered_data = self._add_sector_info(filtered_data)
            
            filtered_data = filtered_data[
                filtered_data['sector'].isin(temp_filter.sectors_filter)
            ]
        
        # Фильтрация по тикерам если указано
        if temp_filter.benchmark_tickers:
            filtered_data = filtered_data[
                filtered_data['ticker'].isin(temp_filter.benchmark_tickers)
            ]
            
        return filtered_data
    
    def _add_sector_info(self, data: pd.DataFrame) -> pd.DataFrame:
        """Добавляет информацию о секторах к данным"""
        try:
            from capital_flow_analyzer import CapitalFlowAnalyzer
            analyzer = CapitalFlowAnalyzer(data)
            sector_mapping = analyzer.sector_mapping
            
            data = data.copy()
            data['sector'] = data['ticker'].map(sector_mapping)
            return data
        except Exception as e:
            self.logger.warning(f"Не удалось добавить информацию о секторах: {e}")
            data['sector'] = 'Неизвестно'
            return data
    
    def calculate_period_performance(self, temp_filter: TemporalFilter) -> Dict[str, any]:
        """
        Рассчитывает показатели производительности за период
        
        Args:
            temp_filter: Временной фильтр
            
        Returns:
            Словарь с показателями производительности
        """
        filtered_data = self.apply_temporal_filter(self.etf_data, temp_filter)
        
        if filtered_data.empty:
            return {}
        
        # Рассчитываем агрегированные показатели
        performance = {
            'period_start': temp_filter.start_date.isoformat(),
            'period_end': temp_filter.end_date.isoformat(),
            'period_days': (temp_filter.end_date - temp_filter.start_date).days,
            'funds_count': len(filtered_data),
            'total_market_cap': filtered_data['market_cap'].sum(),
            'total_volume': filtered_data['avg_daily_volume'].sum(),
            'avg_return': filtered_data['annual_return'].mean(),
            'median_return': filtered_data['annual_return'].median(),
            'avg_volatility': filtered_data['volatility'].mean(),
            'best_performer': self._get_best_performer(filtered_data),
            'worst_performer': self._get_worst_performer(filtered_data),
            'sector_breakdown': self._get_sector_breakdown(filtered_data)
        }
        
        return performance
    
    def _get_best_performer(self, data: pd.DataFrame) -> Dict[str, any]:
        """Находит лучший ETF по доходности"""
        if data.empty:
            return {}
            
        best_idx = data['annual_return'].idxmax()
        best_fund = data.loc[best_idx]
        
        return {
            'ticker': best_fund['ticker'],
            'name': best_fund.get('full_name', ''),
            'return': round(best_fund['annual_return'], 2),
            'volatility': round(best_fund['volatility'], 2),
            'volume': best_fund['avg_daily_volume']
        }
    
    def _get_worst_performer(self, data: pd.DataFrame) -> Dict[str, any]:
        """Находит худший ETF по доходности"""
        if data.empty:
            return {}
            
        worst_idx = data['annual_return'].idxmin()
        worst_fund = data.loc[worst_idx]
        
        return {
            'ticker': worst_fund['ticker'],
            'name': worst_fund.get('full_name', ''),
            'return': round(worst_fund['annual_return'], 2),
            'volatility': round(worst_fund['volatility'], 2),
            'volume': worst_fund['avg_daily_volume']
        }
    
    def _get_sector_breakdown(self, data: pd.DataFrame) -> Dict[str, any]:
        """Анализирует распределение по секторам"""
        if 'sector' not in data.columns:
            data = self._add_sector_info(data)
            
        sector_stats = data.groupby('sector').agg({
            'annual_return': 'mean',
            'volatility': 'mean',
            'avg_daily_volume': 'sum',
            'market_cap': 'sum',
            'ticker': 'count'
        }).round(2)
        
        sector_stats.columns = ['avg_return', 'avg_volatility', 'total_volume', 
                               'total_market_cap', 'funds_count']
        
        return sector_stats.to_dict('index')
    
    def compare_periods(self, period1: TemporalFilter, period2: TemporalFilter) -> Dict[str, any]:
        """
        Сравнивает показатели между двумя периодами
        
        Args:
            period1: Первый период
            period2: Второй период
            
        Returns:
            Результаты сравнения
        """
        perf1 = self.calculate_period_performance(period1)
        perf2 = self.calculate_period_performance(period2)
        
        if not perf1 or not perf2:
            return {}
        
        comparison = {
            'period1': {
                'start': perf1['period_start'],
                'end': perf1['period_end'],
                'performance': perf1
            },
            'period2': {
                'start': perf2['period_start'],
                'end': perf2['period_end'],
                'performance': perf2
            },
            'changes': {
                'return_change': round(perf2['avg_return'] - perf1['avg_return'], 2),
                'volatility_change': round(perf2['avg_volatility'] - perf1['avg_volatility'], 2),
                'volume_change_pct': round(
                    (perf2['total_volume'] - perf1['total_volume']) / perf1['total_volume'] * 100, 2
                ) if perf1['total_volume'] > 0 else 0,
                'market_cap_change_pct': round(
                    (perf2['total_market_cap'] - perf1['total_market_cap']) / perf1['total_market_cap'] * 100, 2
                ) if perf1['total_market_cap'] > 0 else 0
            }
        }
        
        return comparison
    
    def analyze_trend_changes(self, ticker: str, temp_filter: TemporalFilter,
                            window_size: int = 30) -> Dict[str, any]:
        """
        Анализирует изменения трендов для конкретного ETF
        
        Args:
            ticker: Тикер ETF
            temp_filter: Временной фильтр
            window_size: Размер окна для скользящих средних
            
        Returns:
            Анализ изменений трендов
        """
        if not self.historical_manager:
            return {'error': 'Исторические данные недоступны'}
        
        try:
            # Получаем исторические данные
            hist_data = self.historical_manager.load_historical_data(
                ticker, temp_filter.start_date, temp_filter.end_date
            )
            
            if hist_data.empty:
                return {'error': f'Нет данных для {ticker}'}
            
            # Рассчитываем технические индикаторы
            hist_data = hist_data.copy()
            hist_data['ma_short'] = hist_data['close'].rolling(window=window_size//2).mean()
            hist_data['ma_long'] = hist_data['close'].rolling(window=window_size).mean()
            hist_data['volatility_rolling'] = hist_data['close'].pct_change().rolling(window=window_size).std() * np.sqrt(252)
            
            # Определяем тренды
            hist_data['trend_signal'] = np.where(
                hist_data['ma_short'] > hist_data['ma_long'], 1, -1
            )
            
            # Находим изменения трендов
            trend_changes = []
            prev_signal = 0
            
            for idx, row in hist_data.iterrows():
                if not pd.isna(row['trend_signal']) and row['trend_signal'] != prev_signal:
                    trend_changes.append({
                        'date': row['date'].isoformat() if 'date' in row else idx,
                        'price': row['close'],
                        'from_trend': 'Бычий' if prev_signal == 1 else 'Медвежий' if prev_signal == -1 else 'Нейтральный',
                        'to_trend': 'Бычий' if row['trend_signal'] == 1 else 'Медвежий',
                        'volatility': row['volatility_rolling']
                    })
                    prev_signal = row['trend_signal']
            
            # Рассчитываем общую статистику
            total_return = (hist_data['close'].iloc[-1] - hist_data['close'].iloc[0]) / hist_data['close'].iloc[0] * 100
            max_price = hist_data['close'].max()
            min_price = hist_data['close'].min()
            avg_volatility = hist_data['volatility_rolling'].mean()
            
            return {
                'ticker': ticker,
                'period_start': temp_filter.start_date.isoformat(),
                'period_end': temp_filter.end_date.isoformat(),
                'total_return': round(total_return, 2),
                'max_price': round(max_price, 2),
                'min_price': round(min_price, 2),
                'price_range_pct': round((max_price - min_price) / min_price * 100, 2),
                'avg_volatility': round(avg_volatility, 2) if not pd.isna(avg_volatility) else 0,
                'trend_changes_count': len(trend_changes),
                'trend_changes': trend_changes[-10:],  # Последние 10 изменений
                'current_trend': 'Бычий' if hist_data['trend_signal'].iloc[-1] == 1 else 'Медвежий',
                'data_points': len(hist_data)
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка анализа трендов для {ticker}: {e}")
            return {'error': f'Ошибка анализа: {e}'}
    
    def get_crisis_impact_analysis(self) -> Dict[str, any]:
        """
        Анализирует влияние кризисных периодов на БПИФ
        
        Returns:
            Анализ влияния кризисов
        """
        crisis_periods = [
            MarketPeriod.CRISIS_2020,
            MarketPeriod.SANCTIONS_2022
        ]
        
        recovery_periods = [
            MarketPeriod.RECOVERY_2020,
            MarketPeriod.STABILIZATION_2023
        ]
        
        analysis = {
            'crisis_analysis': {},
            'recovery_analysis': {},
            'resilience_ranking': []
        }
        
        # Анализ кризисных периодов
        for period in crisis_periods:
            period_filter = self.get_market_period_filter(period)
            crisis_perf = self.calculate_period_performance(period_filter)
            analysis['crisis_analysis'][period.name] = {
                'description': period.value[2],
                'performance': crisis_perf
            }
        
        # Анализ восстановительных периодов
        for period in recovery_periods:
            period_filter = self.get_market_period_filter(period)
            recovery_perf = self.calculate_period_performance(period_filter)
            analysis['recovery_analysis'][period.name] = {
                'description': period.value[2],
                'performance': recovery_perf
            }
        
        # Рейтинг устойчивости фондов
        analysis['resilience_ranking'] = self._calculate_resilience_ranking()
        
        return analysis
    
    def _calculate_resilience_ranking(self) -> List[Dict[str, any]]:
        """Рассчитывает рейтинг устойчивости фондов"""
        resilience_scores = []
        
        for _, fund in self.etf_data.iterrows():
            # Простая оценка устойчивости на основе доступных данных
            volatility_score = max(0, 100 - fund['volatility'])  # Меньше волатильность = выше балл
            return_score = max(0, fund['annual_return'])  # Положительная доходность
            volume_score = min(100, fund['avg_daily_volume'] / 1000000 * 10)  # Ликвидность
            
            total_score = (volatility_score * 0.4 + return_score * 0.4 + volume_score * 0.2)
            
            resilience_scores.append({
                'ticker': fund['ticker'],
                'name': fund.get('full_name', ''),
                'resilience_score': round(total_score, 1),
                'volatility': round(fund['volatility'], 2),
                'annual_return': round(fund['annual_return'], 2),
                'avg_volume': fund['avg_daily_volume']
            })
        
        # Сортируем по рейтингу устойчивости
        return sorted(resilience_scores, key=lambda x: x['resilience_score'], reverse=True)[:20]
    
    def generate_temporal_insights(self, temp_filter: TemporalFilter) -> Dict[str, any]:
        """
        Генерирует инсайты для указанного временного периода
        
        Args:
            temp_filter: Временной фильтр
            
        Returns:
            Временные инсайты
        """
        performance = self.calculate_period_performance(temp_filter)
        
        if not performance:
            return {}
        
        insights = {
            'period_summary': {
                'start_date': performance['period_start'],
                'end_date': performance['period_end'],
                'duration_days': performance['period_days'],
                'funds_analyzed': performance['funds_count']
            },
            'market_performance': {
                'average_return': performance['avg_return'],
                'median_return': performance['median_return'],
                'average_volatility': performance['avg_volatility'],
                'market_classification': self._classify_market_period(performance)
            },
            'leaders_and_laggards': {
                'best_performer': performance['best_performer'],
                'worst_performer': performance['worst_performer']
            },
            'sector_insights': self._generate_sector_insights(performance['sector_breakdown']),
            'risk_assessment': self._assess_period_risk(performance),
            'recommendations': self._generate_period_recommendations(performance)
        }
        
        return insights
    
    def _classify_market_period(self, performance: Dict[str, any]) -> str:
        """Классифицирует рыночный период"""
        avg_return = performance['avg_return']
        avg_volatility = performance['avg_volatility']
        
        if avg_return > 15 and avg_volatility < 20:
            return "Бычий рынок с низкой волатильностью"
        elif avg_return > 10 and avg_volatility < 30:
            return "Умеренный рост"
        elif avg_return > 0 and avg_volatility > 30:
            return "Рост с высокой волатильностью"
        elif avg_return < -10 and avg_volatility > 25:
            return "Медвежий рынок"
        elif avg_return < 0:
            return "Коррекция"
        else:
            return "Боковой тренд"
    
    def _generate_sector_insights(self, sector_breakdown: Dict[str, any]) -> List[str]:
        """Генерирует инсайты по секторам"""
        insights = []
        
        if not sector_breakdown:
            return insights
        
        # Находим лучший и худший сектора
        sectors_by_return = sorted(
            sector_breakdown.items(),
            key=lambda x: x[1]['avg_return'],
            reverse=True
        )
        
        if sectors_by_return:
            best_sector = sectors_by_return[0]
            worst_sector = sectors_by_return[-1]
            
            insights.append(f"Лучший сектор: {best_sector[0]} (+{best_sector[1]['avg_return']:.1f}%)")
            insights.append(f"Худший сектор: {worst_sector[0]} ({worst_sector[1]['avg_return']:.1f}%)")
        
        # Анализ волатильности по секторам
        low_vol_sectors = [
            name for name, data in sector_breakdown.items()
            if data['avg_volatility'] < 15
        ]
        
        if low_vol_sectors:
            insights.append(f"Низковолатильные сектора: {', '.join(low_vol_sectors[:3])}")
        
        return insights
    
    def _assess_period_risk(self, performance: Dict[str, any]) -> Dict[str, any]:
        """Оценивает риски периода"""
        avg_vol = performance['avg_volatility']
        
        if avg_vol < 15:
            risk_level = "Низкий"
        elif avg_vol < 25:
            risk_level = "Умеренный"
        elif avg_vol < 35:
            risk_level = "Повышенный"
        else:
            risk_level = "Высокий"
        
        return {
            'risk_level': risk_level,
            'average_volatility': avg_vol,
            'risk_factors': self._identify_risk_factors(performance)
        }
    
    def _identify_risk_factors(self, performance: Dict[str, any]) -> List[str]:
        """Идентифицирует факторы риска"""
        factors = []
        
        if performance['avg_volatility'] > 30:
            factors.append("Высокая волатильность рынка")
        
        if performance['avg_return'] < -5:
            factors.append("Отрицательная доходность")
        
        if performance['funds_count'] < 20:
            factors.append("Ограниченная выборка фондов")
        
        return factors
    
    def _generate_period_recommendations(self, performance: Dict[str, any]) -> List[str]:
        """Генерирует рекомендации на основе анализа периода"""
        recommendations = []
        
        if performance['avg_return'] > 10:
            recommendations.append("Период показывает хорошие возможности для роста")
        
        if performance['avg_volatility'] < 20:
            recommendations.append("Низкая волатильность создает стабильные условия")
        
        if performance['best_performer']['return'] > 20:
            best_ticker = performance['best_performer']['ticker']
            recommendations.append(f"Обратите внимание на лидера {best_ticker}")
        
        return recommendations

def main():
    """Тестирование движка временного анализа"""
    
    try:
        # Загружаем данные ETF
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
            
        if not data_files:
            print("❌ Файлы с данными ETF не найдены")
            return
            
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        print(f"📊 Загружаем данные из {latest_data}")
        
        etf_data = pd.read_csv(latest_data)
        print(f"✅ Загружено {len(etf_data)} ETF")
        
        # Создаем движок анализа
        engine = TemporalAnalysisEngine(etf_data)
        
        # Тестируем различные временные фильтры
        print("\n🔍 Тестирование временных фильтров...")
        
        # Анализ кризиса 2020
        crisis_filter = engine.get_market_period_filter(MarketPeriod.CRISIS_2020)
        crisis_performance = engine.calculate_period_performance(crisis_filter)
        print(f"\n📉 Кризис 2020 (COVID-19):")
        print(f"   Средняя доходность: {crisis_performance.get('avg_return', 0):.1f}%")
        print(f"   Средняя волатильность: {crisis_performance.get('avg_volatility', 0):.1f}%")
        
        # Анализ восстановления 2020
        recovery_filter = engine.get_market_period_filter(MarketPeriod.RECOVERY_2020)
        recovery_performance = engine.calculate_period_performance(recovery_filter)
        print(f"\n📈 Восстановление 2020:")
        print(f"   Средняя доходность: {recovery_performance.get('avg_return', 0):.1f}%")
        print(f"   Средняя волатильность: {recovery_performance.get('avg_volatility', 0):.1f}%")
        
        # Сравнение периодов
        comparison = engine.compare_periods(crisis_filter, recovery_filter)
        if comparison:
            changes = comparison['changes']
            print(f"\n🔄 Сравнение периодов:")
            print(f"   Изменение доходности: {changes['return_change']:+.1f}%")
            print(f"   Изменение волатильности: {changes['volatility_change']:+.1f}%")
        
        # Анализ устойчивости
        crisis_impact = engine.get_crisis_impact_analysis()
        top_resilient = crisis_impact['resilience_ranking'][:5]
        print(f"\n🛡️  Топ-5 самых устойчивых фондов:")
        for fund in top_resilient:
            print(f"   {fund['ticker']}: {fund['resilience_score']:.1f} баллов")
        
        # Генерируем инсайты для текущего периода
        current_filter = engine.get_market_period_filter(MarketPeriod.CURRENT_2024)
        insights = engine.generate_temporal_insights(current_filter)
        
        if insights:
            print(f"\n💡 Инсайты для текущего периода:")
            market_perf = insights['market_performance']
            print(f"   Классификация рынка: {market_perf['market_classification']}")
            print(f"   Средняя доходность: {market_perf['average_return']:.1f}%")
            
            if insights['recommendations']:
                print(f"   Рекомендации:")
                for rec in insights['recommendations']:
                    print(f"   - {rec}")
        
        print(f"\n✅ Тестирование временного анализа завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

if __name__ == "__main__":
    main()