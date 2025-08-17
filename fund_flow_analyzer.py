#!/usr/bin/env python3
"""
Анализатор перетоков активов между БПИФ
Отслеживает изменения размеров фондов и миграцию капитала
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
from pathlib import Path
import logging
from dataclasses import dataclass

@dataclass
class FlowAnalysisResult:
    """Результат анализа перетоков"""
    period_start: datetime
    period_end: datetime
    fund_flows: pd.DataFrame
    sector_flows: Dict[str, any]
    net_flows: Dict[str, float]
    flow_winners: List[Dict[str, any]]
    flow_losers: List[Dict[str, any]]
    total_flow_volume: float

class FundFlowAnalyzer:
    """Анализатор перетоков активов между БПИФ"""
    
    def __init__(self, historical_manager=None):
        self.historical_manager = historical_manager
        self.logger = self._setup_logger()
        self._cached_data = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('FundFlowAnalyzer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def analyze_fund_size_changes(self, current_data: pd.DataFrame, 
                                 period_start: datetime, period_end: datetime) -> pd.DataFrame:
        """
        Анализирует изменения размеров фондов за период
        
        Args:
            current_data: Текущие данные о фондах
            period_start: Начало периода анализа
            period_end: Конец периода анализа
            
        Returns:
            DataFrame с изменениями размеров фондов
        """
        fund_changes = []
        
        for _, fund in current_data.iterrows():
            ticker = fund['ticker']
            
            # Получаем исторические данные для фонда
            try:
                if self.historical_manager:
                    hist_data = self.historical_manager.load_historical_data(
                        ticker, period_start, period_end
                    )
                    
                    if not hist_data.empty and len(hist_data) >= 2:
                        # Анализируем изменения объемов торгов как прокси для притоков
                        start_volume = hist_data['volume'].head(5).mean()  # Средний объем в начале
                        end_volume = hist_data['volume'].tail(5).mean()    # Средний объем в конце
                        
                        volume_change = (end_volume - start_volume) / start_volume * 100 if start_volume > 0 else 0
                        
                        # Анализируем ценовую динамику
                        start_price = hist_data['close'].iloc[0]
                        end_price = hist_data['close'].iloc[-1]
                        price_change = (end_price - start_price) / start_price * 100 if start_price > 0 else 0
                        
                        # Оценка притока/оттока на основе объемов и цены
                        flow_indicator = self._calculate_flow_indicator(hist_data)
                        
                        fund_changes.append({
                            'ticker': ticker,
                            'name': fund.get('full_name', fund.get('short_name', ticker)),
                            'current_market_cap': fund.get('market_cap', 0),
                            'current_volume': fund.get('avg_daily_volume', 0),
                            'period_return': price_change,
                            'volume_change': volume_change,
                            'flow_indicator': flow_indicator,
                            'flow_direction': self._classify_flow(flow_indicator, volume_change),
                            'data_points': len(hist_data)
                        })
                else:
                    # Если нет исторических данных, используем текущие метрики
                    fund_changes.append({
                        'ticker': ticker,
                        'name': fund.get('full_name', fund.get('short_name', ticker)),
                        'current_market_cap': fund.get('market_cap', 0),
                        'current_volume': fund.get('avg_daily_volume', 0),
                        'period_return': fund.get('annual_return', 0),
                        'volume_change': 0,
                        'flow_indicator': 0,
                        'flow_direction': 'Неизвестно',
                        'data_points': 0
                    })
                    
            except Exception as e:
                self.logger.warning(f"Ошибка анализа {ticker}: {e}")
                continue
        
        return pd.DataFrame(fund_changes)
    
    def _calculate_flow_indicator(self, hist_data: pd.DataFrame) -> float:
        """
        Рассчитывает индикатор потока на основе исторических данных
        
        Использует комбинацию:
        - Изменение объемов торгов
        - Ценовая динамика
        - Волатильность
        """
        if hist_data.empty or len(hist_data) < 5:
            return 0
        
        try:
            # Тренд объемов
            volumes = hist_data['volume'].rolling(window=5).mean()
            volume_trend = (volumes.iloc[-1] - volumes.iloc[0]) / volumes.iloc[0] if volumes.iloc[0] > 0 else 0
            
            # Ценовой тренд
            prices = hist_data['close']
            price_trend = (prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0] if prices.iloc[0] > 0 else 0
            
            # Устойчивость тренда (низкая волатильность = более устойчивый приток)
            returns = prices.pct_change().dropna()
            volatility = returns.std() if len(returns) > 1 else 1
            stability_factor = 1 / (1 + volatility) if volatility > 0 else 1
            
            # Комбинированный индикатор
            flow_indicator = (volume_trend * 0.4 + price_trend * 0.4) * stability_factor * 100
            
            return flow_indicator
            
        except Exception as e:
            self.logger.warning(f"Ошибка расчета flow indicator: {e}")
            return 0
    
    def _classify_flow(self, flow_indicator: float, volume_change: float) -> str:
        """Классифицирует направление потока"""
        
        if flow_indicator > 5 and volume_change > 10:
            return "Сильный приток"
        elif flow_indicator > 2:
            return "Приток"
        elif flow_indicator < -5 and volume_change < -10:
            return "Сильный отток"
        elif flow_indicator < -2:
            return "Отток"
        else:
            return "Стабильный"
    
    def analyze_sector_flows(self, fund_flows: pd.DataFrame, etf_data: pd.DataFrame) -> Dict[str, any]:
        """
        Анализирует перетоки по секторам
        
        Args:
            fund_flows: Результаты анализа фондов
            etf_data: Исходные данные ETF с секторной разбивкой
            
        Returns:
            Анализ перетоков по секторам
        """
        try:
            # Добавляем информацию о секторах
            from capital_flow_analyzer import CapitalFlowAnalyzer
            analyzer = CapitalFlowAnalyzer(etf_data)
            sector_mapping = analyzer.sector_mapping
            
            fund_flows['sector'] = fund_flows['ticker'].map(sector_mapping)
            
            # Агрегируем по секторам
            sector_flows = fund_flows.groupby('sector').agg({
                'flow_indicator': 'mean',
                'volume_change': 'mean',
                'current_market_cap': 'sum',
                'current_volume': 'sum',
                'ticker': 'count'
            }).round(2)
            
            sector_flows.columns = [
                'avg_flow_indicator', 'avg_volume_change', 'total_market_cap',
                'total_volume', 'funds_count'
            ]
            
            # Классифицируем секторы
            sector_flows['flow_category'] = sector_flows['avg_flow_indicator'].apply(
                lambda x: 'Приток' if x > 2 else 'Отток' if x < -2 else 'Стабильный'
            )
            
            # Рассчитываем долю рынка
            total_market_cap = sector_flows['total_market_cap'].sum()
            sector_flows['market_share'] = (
                sector_flows['total_market_cap'] / total_market_cap * 100
            ).round(1) if total_market_cap > 0 else 0
            
            return {
                'sector_summary': sector_flows.to_dict('index'),
                'inflow_sectors': sector_flows[sector_flows['avg_flow_indicator'] > 2].index.tolist(),
                'outflow_sectors': sector_flows[sector_flows['avg_flow_indicator'] < -2].index.tolist(),
                'stable_sectors': sector_flows[
                    (sector_flows['avg_flow_indicator'] >= -2) & 
                    (sector_flows['avg_flow_indicator'] <= 2)
                ].index.tolist()
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка анализа секторных потоков: {e}")
            return {}
    
    def identify_flow_winners_losers(self, fund_flows: pd.DataFrame, 
                                   top_n: int = 10) -> Tuple[List[Dict], List[Dict]]:
        """
        Определяет фонды-победители и фонды-аутсайдеры по потокам
        
        Args:
            fund_flows: Результаты анализа фондов
            top_n: Количество фондов в топе
            
        Returns:
            Tuple (победители, аутсайдеры)
        """
        # Фильтруем фонды с достаточным количеством данных
        valid_funds = fund_flows[fund_flows['data_points'] >= 5].copy()
        
        if valid_funds.empty:
            return [], []
        
        # Сортируем по flow_indicator
        winners = valid_funds.nlargest(top_n, 'flow_indicator').to_dict('records')
        losers = valid_funds.nsmallest(top_n, 'flow_indicator').to_dict('records')
        
        return winners, losers
    
    def analyze_cross_sector_flows(self, fund_flows: pd.DataFrame, 
                                 etf_data: pd.DataFrame) -> Dict[str, any]:
        """
        Анализирует перетоки между секторами
        
        Args:
            fund_flows: Результаты анализа фондов
            etf_data: Исходные данные ETF
            
        Returns:
            Анализ межсекторных перетоков
        """
        try:
            sector_analysis = self.analyze_sector_flows(fund_flows, etf_data)
            
            if not sector_analysis:
                return {}
            
            inflow_sectors = sector_analysis.get('inflow_sectors', [])
            outflow_sectors = sector_analysis.get('outflow_sectors', [])
            
            # Анализируем возможные направления перетоков
            flow_patterns = []
            
            # Сопоставляем оттоки и притоки
            for outflow_sector in outflow_sectors:
                for inflow_sector in inflow_sectors:
                    if outflow_sector != inflow_sector:
                        
                        # Рассчитываем потенциальный объем перетока
                        outflow_data = sector_analysis['sector_summary'].get(outflow_sector, {})
                        inflow_data = sector_analysis['sector_summary'].get(inflow_sector, {})
                        
                        if outflow_data and inflow_data:
                            potential_flow = min(
                                abs(outflow_data.get('avg_flow_indicator', 0)),
                                inflow_data.get('avg_flow_indicator', 0)
                            )
                            
                            if potential_flow > 1:  # Значимый поток
                                flow_patterns.append({
                                    'from_sector': outflow_sector,
                                    'to_sector': inflow_sector,
                                    'flow_strength': potential_flow,
                                    'from_market_cap': outflow_data.get('total_market_cap', 0),
                                    'to_market_cap': inflow_data.get('total_market_cap', 0)
                                })
            
            # Сортируем по силе потока
            flow_patterns.sort(key=lambda x: x['flow_strength'], reverse=True)
            
            return {
                'cross_sector_flows': flow_patterns[:10],  # Топ-10 перетоков
                'dominant_flow_direction': self._identify_dominant_flow_direction(flow_patterns),
                'total_flow_patterns': len(flow_patterns)
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка анализа межсекторных потоков: {e}")
            return {}
    
    def _identify_dominant_flow_direction(self, flow_patterns: List[Dict]) -> str:
        """Определяет доминирующее направление потоков"""
        
        if not flow_patterns:
            return "Нет значимых потоков"
        
        # Анализируем топ-3 потока
        top_flows = flow_patterns[:3]
        
        # Ищем общие паттерны
        common_sources = {}
        common_targets = {}
        
        for flow in top_flows:
            source = flow['from_sector']
            target = flow['to_sector']
            
            common_sources[source] = common_sources.get(source, 0) + 1
            common_targets[target] = common_targets.get(target, 0) + 1
        
        # Определяем доминирующий паттерн
        max_source = max(common_sources.items(), key=lambda x: x[1]) if common_sources else None
        max_target = max(common_targets.items(), key=lambda x: x[1]) if common_targets else None
        
        if max_source and max_target:
            return f"Отток из {max_source[0]} → приток в {max_target[0]}"
        else:
            return "Разнонаправленные потоки"
    
    def generate_flow_report(self, current_data: pd.DataFrame, 
                           period_start: datetime, period_end: datetime) -> FlowAnalysisResult:
        """
        Генерирует полный отчет по потокам активов
        
        Args:
            current_data: Текущие данные о фондах
            period_start: Начало периода анализа
            period_end: Конец периода анализа
            
        Returns:
            Полный анализ потоков
        """
        self.logger.info(f"Генерация отчета по потокам за период {period_start} - {period_end}")
        
        # Анализируем изменения размеров фондов
        fund_flows = self.analyze_fund_size_changes(current_data, period_start, period_end)
        
        # Анализируем потоки по секторам
        sector_flows = self.analyze_sector_flows(fund_flows, current_data)
        
        # Определяем победителей и аутсайдеров
        winners, losers = self.identify_flow_winners_losers(fund_flows)
        
        # Рассчитываем общие потоки
        total_inflows = fund_flows[fund_flows['flow_indicator'] > 0]['flow_indicator'].sum()
        total_outflows = abs(fund_flows[fund_flows['flow_indicator'] < 0]['flow_indicator'].sum())
        net_flows = total_inflows - total_outflows
        
        # Анализируем межсекторные потоки
        cross_sector_analysis = self.analyze_cross_sector_flows(fund_flows, current_data)
        
        return FlowAnalysisResult(
            period_start=period_start,
            period_end=period_end,
            fund_flows=fund_flows,
            sector_flows={
                **sector_flows,
                **cross_sector_analysis
            },
            net_flows={
                'total_inflows': total_inflows,
                'total_outflows': total_outflows,
                'net_flow': net_flows
            },
            flow_winners=winners,
            flow_losers=losers,
            total_flow_volume=total_inflows + total_outflows
        )

def main():
    """Тестирование анализатора потоков"""
    
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
        
        # Создаем анализатор
        from historical_data_manager import HistoricalDataManager
        hist_manager = HistoricalDataManager()
        analyzer = FundFlowAnalyzer(hist_manager)
        
        # Анализируем потоки за последние 3 месяца
        period_end = datetime.now()
        period_start = period_end - timedelta(days=90)
        
        print(f"\n🔄 Анализ потоков за период {period_start.strftime('%Y-%m-%d')} - {period_end.strftime('%Y-%m-%d')}")
        
        # Генерируем отчет
        report = analyzer.generate_flow_report(etf_data, period_start, period_end)
        
        print(f"\n💰 Общие потоки:")
        print(f"   Притоки: {report.net_flows['total_inflows']:.1f}")
        print(f"   Оттоки: {report.net_flows['total_outflows']:.1f}")
        print(f"   Чистый поток: {report.net_flows['net_flow']:.1f}")
        print(f"   Общий объем: {report.total_flow_volume:.1f}")
        
        print(f"\n🏆 Топ-5 фондов по притокам:")
        for i, winner in enumerate(report.flow_winners[:5], 1):
            print(f"   {i}. {winner['ticker']}: {winner['flow_indicator']:.1f} ({winner['flow_direction']})")
        
        print(f"\n📉 Топ-5 фондов по оттокам:")
        for i, loser in enumerate(report.flow_losers[:5], 1):
            print(f"   {i}. {loser['ticker']}: {loser['flow_indicator']:.1f} ({loser['flow_direction']})")
        
        if 'inflow_sectors' in report.sector_flows:
            inflow_sectors = report.sector_flows['inflow_sectors']
            outflow_sectors = report.sector_flows['outflow_sectors']
            
            print(f"\n🏢 Секторы с притоком: {', '.join(inflow_sectors[:3])}")
            print(f"🏢 Секторы с оттоком: {', '.join(outflow_sectors[:3])}")
        
        if 'cross_sector_flows' in report.sector_flows:
            cross_flows = report.sector_flows['cross_sector_flows']
            if cross_flows:
                print(f"\n🔄 Топ межсекторный поток:")
                top_flow = cross_flows[0]
                print(f"   {top_flow['from_sector']} → {top_flow['to_sector']} (сила: {top_flow['flow_strength']:.1f})")
        
        print(f"\n✅ Анализ потоков завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка анализа потоков: {e}")

if __name__ == "__main__":
    main()