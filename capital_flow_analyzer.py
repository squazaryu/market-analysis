#!/usr/bin/env python3
"""
Анализатор перетоков капитала и трендов для рынка БПИФ
Отслеживает движение денег между секторами при изменении геополитической ситуации
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
from pathlib import Path
from full_fund_compositions import get_fund_category, FUND_COMPOSITIONS
from historical_data_manager import HistoricalDataManager

class CapitalFlowAnalyzer:
    """Анализатор перетоков капитала между секторами ETF"""
    
    def __init__(self, etf_data: pd.DataFrame, historical_manager: Optional[HistoricalDataManager] = None):
        self.etf_data = etf_data
        self.historical_manager = historical_manager or HistoricalDataManager()
        self.asset_type_mapping = self._create_asset_type_mapping()
        
    def _create_asset_type_mapping(self) -> Dict[str, str]:
        """Создает маппинг ETF по типам активов (как в секторальном анализе)"""
        asset_type_map = {}
        
        def group_by_asset_type(sector, ticker='', name=''):
            sector_lower = sector.lower()
            name_lower = name.lower() if name else ''
            
            # Специальная обработка валютных фондов
            if 'валютн' in sector_lower or 'валют' in sector_lower:
                if 'облигации' in name_lower or 'облигац' in name_lower:
                    return 'Облигации'
                elif ('ликвидность' in name_lower or 'накопительный' in name_lower or 
                      'сберегательный' in name_lower):
                    return 'Денежный рынок'
                else:
                    return 'Смешанные'
            
            # Антиинфляционные фонды относим к смешанным
            elif 'защитн' in sector_lower or 'антиинфляц' in sector_lower:
                return 'Смешанные'
            
            # Драгоценные металлы остаются товарами
            elif 'золот' in sector_lower or 'драгоценн' in sector_lower or 'металл' in sector_lower:
                return 'Товары'
            
            # Остальные категории без изменений
            elif 'акци' in sector_lower:
                return 'Акции'
            elif 'облига' in sector_lower:
                return 'Облигации'
            elif 'денежн' in sector_lower or 'ликвидн' in sector_lower:
                return 'Денежный рынок'
            elif 'смешанн' in sector_lower or 'диверс' in sector_lower:
                return 'Смешанные'
            else:
                return 'Прочие'
        
        # Применяем группировку к каждому ETF
        for _, row in self.etf_data.iterrows():
            ticker = row['ticker']
            sector = row.get('sector', '')
            name = row.get('name', '')
            
            asset_type_map[ticker] = group_by_asset_type(sector, ticker, name)
                
        return asset_type_map
    
    def calculate_real_capital_flows(self, days: int = 30) -> pd.DataFrame:
        """Рассчитывает реальные притоки/оттоки капитала через изменения СЧА"""
        
        flows_data = []
        
        for _, row in self.etf_data.iterrows():
            ticker = row['ticker']
            current_nav = row.get('market_cap', 0)
            
            # Получаем тип активов
            asset_type = self.asset_type_mapping.get(ticker, 'Прочие')
            
            try:
                # Пытаемся получить исторические данные СЧА
                if hasattr(self.historical_manager, 'get_nav_history'):
                    nav_history = self.historical_manager.get_nav_history(ticker, days=days)
                else:
                    nav_history = None
                
                if nav_history is not None and len(nav_history) >= 2:
                    # Есть исторические данные - считаем реальный поток
                    nav_start = nav_history.iloc[0].get('nav', current_nav)
                    nav_end = nav_history.iloc[-1].get('nav', current_nav)
                    
                    # Исключаем влияние изменения цены пая (рыночный рост/падение)
                    price_start = nav_history.iloc[0].get('price', 100)
                    price_end = nav_history.iloc[-1].get('price', 100)
                    
                    if price_start > 0:
                        market_growth = (price_end / price_start - 1)
                        expected_nav = nav_start * (1 + market_growth)
                        
                        # Реальный приток/отток = разница между фактическим и ожидаемым СЧА
                        net_flow = nav_end - expected_nav
                        flow_percent = (net_flow / nav_start) * 100 if nav_start > 0 else 0
                    else:
                        net_flow = nav_end - nav_start
                        flow_percent = (net_flow / nav_start) * 100 if nav_start > 0 else 0
                        
                else:
                    # Нет исторических данных - используем упрощенную оценку на основе доходности
                    annual_return = row.get('annual_return', 0)
                    period_return = annual_return * (days / 365)
                    
                    # Если доходность существенно отличается от среднерыночной, 
                    # это может указывать на притоки/оттоки
                    market_avg_return = self.etf_data['annual_return'].mean()
                    period_market_return = market_avg_return * (days / 365)
                    
                    excess_return = period_return - period_market_return
                    
                    # Эмпирическая оценка: избыточная доходность может указывать на потоки
                    flow_percent = excess_return * 0.3  # Коэффициент корреляции потоков и доходности
                    net_flow = (flow_percent / 100) * current_nav if current_nav > 0 else 0
                
            except Exception as e:
                # В случае ошибки используем нулевые значения
                net_flow = 0
                flow_percent = 0
            
            flows_data.append({
                'ticker': ticker,
                'asset_type': asset_type,
                'nav_current': current_nav,
                'net_flow_rub': net_flow,
                'flow_percent': round(flow_percent, 2),
                'flow_direction': 'Приток' if net_flow > 0 else ('Отток' if net_flow < 0 else 'Нейтрально'),
                'flow_intensity': abs(flow_percent)
            })
        
        flows_df = pd.DataFrame(flows_data)
        
        # Группируем по типам активов
        asset_flows = flows_df.groupby('asset_type').agg({
            'nav_current': 'sum',
            'net_flow_rub': 'sum',
            'flow_percent': 'mean',
            'flow_intensity': 'mean',
            'ticker': 'count'
        }).round(2)
        
        asset_flows.columns = [
            'total_nav', 'total_net_flow', 'avg_flow_percent', 
            'avg_flow_intensity', 'funds_count'
        ]
        
        # Рассчитываем долю каждого типа активов
        total_nav = asset_flows['total_nav'].sum()
        if total_nav > 0:
            asset_flows['nav_share'] = (asset_flows['total_nav'] / total_nav * 100).round(1)
            asset_flows['flow_share'] = (asset_flows['total_net_flow'] / asset_flows['total_net_flow'].abs().sum() * 100).round(1)
        else:
            asset_flows['nav_share'] = 0
            asset_flows['flow_share'] = 0
        
        # Определяем направление потока для каждого типа активов
        asset_flows['flow_direction'] = asset_flows['total_net_flow'].apply(
            lambda x: 'Приток' if x > 0 else ('Отток' if x < 0 else 'Нейтрально')
        )
        
        return asset_flows.sort_values('total_net_flow', ascending=False)
    
    def calculate_sector_flows(self) -> pd.DataFrame:
        """Рассчитывает потоки капитала по секторам (типам активов)"""
        
        # Используем уже реализованный метод calculate_real_capital_flows
        asset_flows = self.calculate_real_capital_flows()
        
        # Переименовываем для совместимости с остальным кодом
        sector_flows = asset_flows.copy()
        sector_flows.rename(columns={
            'total_nav': 'volume_share',
            'total_net_flow': 'net_flow', 
            'avg_flow_percent': 'avg_return',
            'funds_count': 'etf_count'
        }, inplace=True)
        
        # Добавляем долю рынка в процентах
        total_volume = sector_flows['volume_share'].sum()
        if total_volume > 0:
            sector_flows['market_cap_share'] = (sector_flows['volume_share'] / total_volume * 100).round(1)
        else:
            sector_flows['market_cap_share'] = 0
            
        return sector_flows.sort_values('volume_share', ascending=False)
    
    def analyze_sector_momentum(self) -> pd.DataFrame:
        """Анализирует моментум по секторам"""
        
        sector_flows = self.calculate_sector_flows()
        
        # Рассчитываем моментум на основе потоков капитала и доходности
        momentum_data = []
        
        for sector, row in sector_flows.iterrows():
            # Базовый моментум = комбинация потока капитала и доходности
            flow_momentum = row['net_flow'] / max(abs(sector_flows['net_flow']).max(), 1) * 50
            return_momentum = row['avg_return'] * 2  # увеличиваем влияние доходности
            
            # Общий моментум 
            momentum_score = (flow_momentum + return_momentum) / 2
            
            # Определяем тренд
            if momentum_score > 10:
                trend = 'Восходящий'
            elif momentum_score < -10:
                trend = 'Нисходящий'  
            else:
                trend = 'Боковой'
                
            momentum_data.append({
                'sector': sector,
                'momentum_score': round(momentum_score, 1),
                'momentum_trend': trend,
                'flow_component': round(flow_momentum, 1),
                'return_component': round(return_momentum, 1)
            })
        
        momentum_df = pd.DataFrame(momentum_data)
        momentum_df.set_index('sector', inplace=True)
        
        return momentum_df.sort_values('momentum_score', ascending=False)
    
    def detect_flow_anomalies(self) -> List[Dict[str, any]]:
        """Выявляет аномальные потоки капитала"""
        
        asset_flows = self.calculate_real_capital_flows()
        anomalies = []
        
        # Пороги для выявления аномалий
        volume_threshold = asset_flows['total_nav'].quantile(0.8)  # Топ-20% по объему
        flow_threshold = asset_flows['total_net_flow'].abs().quantile(0.7)  # Топ-30% по потокам
        
        for sector, row in asset_flows.iterrows():
            # Аномалии по объему потоков
            if abs(row['total_net_flow']) > flow_threshold:
                anomalies.append({
                    'type': 'Аномальный поток',
                    'sector': sector,
                    'value': abs(row['total_net_flow']) / 1e9,  # в млрд руб
                    'direction': 'Приток' if row['total_net_flow'] > 0 else 'Отток',
                    'threshold': round(flow_threshold / 1e9, 1),
                    'severity': 'Высокая' if abs(row['total_net_flow']) > flow_threshold * 1.5 else 'Средняя'
                })
            
            # Аномалии по доходности при больших объемах
            if row['total_nav'] > volume_threshold and abs(row['avg_flow_percent']) > 5:
                anomalies.append({
                    'type': 'Аномальная доходность',
                    'sector': sector, 
                    'value': abs(row['avg_flow_percent']),
                    'direction': 'Рост' if row['avg_flow_percent'] > 0 else 'Падение',
                    'threshold': 5.0,
                    'severity': 'Высокая' if abs(row['avg_flow_percent']) > 10 else 'Средняя'
                })
        
        return sorted(anomalies, key=lambda x: x['value'], reverse=True)
    
    def detect_risk_sentiment(self) -> Dict[str, any]:
        """Определяет рыночные настроения (risk-on/risk-off) на основе потоков капитала"""
        
        asset_flows = self.calculate_real_capital_flows()
        
        # Защитные активы (risk-off) - инвесторы ищут стабильность
        defensive_assets = ['Облигации', 'Денежный рынок', 'Товары']  # Товары включают золото
        # Рисковые активы (risk-on) - инвесторы готовы к риску ради доходности
        risky_assets = ['Акции']
        # Смешанные активы - промежуточная категория
        mixed_assets = ['Смешанные']
        
        # Рассчитываем потоки по категориям
        defensive_flow = asset_flows[asset_flows.index.isin(defensive_assets)]['total_net_flow'].sum()
        risky_flow = asset_flows[asset_flows.index.isin(risky_assets)]['total_net_flow'].sum()
        mixed_flow = asset_flows[asset_flows.index.isin(mixed_assets)]['total_net_flow'].sum()
        
        # Рассчитываем доли активов (по СЧА)
        defensive_nav = asset_flows[asset_flows.index.isin(defensive_assets)]['nav_share'].sum()
        risky_nav = asset_flows[asset_flows.index.isin(risky_assets)]['nav_share'].sum()
        mixed_nav = asset_flows[asset_flows.index.isin(mixed_assets)]['nav_share'].sum()
        
        # Общий объем потоков для нормализации
        total_flow = abs(defensive_flow) + abs(risky_flow) + abs(mixed_flow)
        
        # Улучшенная логика определения настроений
        sentiment_score = 0
        confidence = 0
        
        if total_flow > 0:
            # Risk-Off индикаторы: приток в защитные активы, отток из рисковых
            risk_off_score = (defensive_flow / total_flow * 100) - (risky_flow / total_flow * 100)
            
            # Risk-On индикаторы: приток в рисковые активы, отток из защитных
            risk_on_score = (risky_flow / total_flow * 100) - (defensive_flow / total_flow * 100)
            
            # Учитываем объемы притоков/оттоков
            flow_intensity = total_flow / 1e9  # в млрд руб
            
            if risk_off_score > 20 and defensive_flow > 0:
                sentiment = "Risk-Off"
                sentiment_score = risk_off_score
                confidence = min(95, max(40, risk_off_score + flow_intensity * 10))
            elif risk_on_score > 20 and risky_flow > 0:
                sentiment = "Risk-On" 
                sentiment_score = risk_on_score
                confidence = min(95, max(40, risk_on_score + flow_intensity * 10))
            elif abs(risk_off_score) <= 20 and abs(risk_on_score) <= 20:
                sentiment = "Нейтральный"
                sentiment_score = 0
                confidence = max(20, 50 - abs(risk_off_score - risk_on_score))
            elif defensive_flow < 0 and risky_flow < 0:
                # Оттоки из всех активов - паника или переключение в кэш
                sentiment = "Неопределенный"
                sentiment_score = -abs(defensive_flow + risky_flow) / total_flow * 100
                confidence = 60
            else:
                # Смешанные сигналы
                if abs(defensive_flow) > abs(risky_flow):
                    sentiment = "Risk-Off" if defensive_flow > 0 else "Осторожный"
                    sentiment_score = defensive_flow / total_flow * 100
                else:
                    sentiment = "Risk-On" if risky_flow > 0 else "Осторожный"
                    sentiment_score = risky_flow / total_flow * 100
                confidence = 40
        else:
            # Нет значимых потоков
            sentiment = "Застой"
            confidence = 20
            sentiment_score = 0
        
        return {
            'sentiment': sentiment,
            'confidence': round(confidence, 1),
            'sentiment_score': round(sentiment_score, 1),
            'defensive_flow': round(defensive_flow / 1e9, 2),  # в млрд руб
            'risky_flow': round(risky_flow / 1e9, 2),         # в млрд руб
            'mixed_flow': round(mixed_flow / 1e9, 2),         # в млрд руб
            'total_flow': round(total_flow / 1e9, 2),         # в млрд руб
            'defensive_share': round(defensive_nav, 1),
            'risky_share': round(risky_nav, 1),
            'mixed_share': round(mixed_nav, 1),
            'flow_intensity': 'Высокая' if total_flow > 2e9 else 'Средняя' if total_flow > 0.5e9 else 'Низкая',
            'analysis_period': '30 дней'
        }
    
    
    def generate_flow_insights(self) -> Dict[str, any]:
        """Генерирует инсайты по потокам капитала"""
        
        sector_flows = self.calculate_sector_flows()
        sentiment = self.detect_risk_sentiment()
        momentum = self.analyze_sector_momentum()
        anomalies = self.detect_flow_anomalies()
        
        # Топ секторы по объему
        top_sectors = sector_flows.head(3).index.tolist()
        
        # Секторы с лучшим моментумом
        momentum_leaders = momentum.head(3).index.tolist()
        
        insights = {
            'market_sentiment': sentiment,
            'top_volume_sectors': top_sectors,
            'momentum_leaders': momentum_leaders,
            'total_anomalies': len(anomalies),
            'critical_anomalies': len([a for a in anomalies if a['severity'] == 'Высокая']),
            'sector_count': len(sector_flows),
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        return insights
    
    def analyze_fund_flows(self) -> pd.DataFrame:
        """Анализирует перетоки между конкретными фондами"""
        
        # Создаем DataFrame с информацией о фондах
        fund_flows = self.etf_data.copy()
        fund_flows['sector'] = fund_flows['ticker'].map(
            lambda x: self.sector_mapping.get(x, 'Смешанные/Прочие')
        )
        
        # Рассчитываем метрики для каждого фонда
        fund_flows['flow_score'] = (
            fund_flows['avg_daily_volume'] / fund_flows['avg_daily_volume'].max() * 50 +
            abs(fund_flows['annual_return']) / abs(fund_flows['annual_return']).max() * 30 +
            (100 - fund_flows['volatility']) / 100 * 20
        ).round(1)
        
        # Определяем направление потока (приток/отток)
        fund_flows['flow_direction'] = fund_flows.apply(lambda row: 
            'Приток' if row['annual_return'] > 0 and row['avg_daily_volume'] > fund_flows['avg_daily_volume'].median()
            else 'Отток' if row['annual_return'] < 0 and row['avg_daily_volume'] > fund_flows['avg_daily_volume'].median()
            else 'Стабильный', axis=1
        )
        
        # Сортируем по объему торгов
        fund_flows = fund_flows.sort_values('avg_daily_volume', ascending=False)
        
        return fund_flows[['ticker', 'full_name', 'sector', 'avg_daily_volume', 
                          'annual_return', 'volatility', 'flow_score', 'flow_direction']]
    
    def detect_sector_rotation(self) -> Dict[str, any]:
        """Выявляет ротацию между секторами"""
        
        sector_flows = self.calculate_sector_flows()
        fund_flows = self.analyze_fund_flows()
        
        # Анализируем потоки по секторам
        inflow_sectors = []
        outflow_sectors = []
        
        for sector in sector_flows.index:
            sector_funds = fund_flows[fund_flows['sector'] == sector]
            
            if len(sector_funds) > 0:
                inflow_count = len(sector_funds[sector_funds['flow_direction'] == 'Приток'])
                outflow_count = len(sector_funds[sector_funds['flow_direction'] == 'Отток'])
                total_volume = sector_funds['avg_daily_volume'].sum()
                
                if inflow_count > outflow_count and total_volume > 0:
                    inflow_sectors.append({
                        'sector': sector,
                        'net_flow': inflow_count - outflow_count,
                        'volume': total_volume,
                        'funds_count': len(sector_funds)
                    })
                elif outflow_count > inflow_count:
                    outflow_sectors.append({
                        'sector': sector,
                        'net_flow': outflow_count - inflow_count,
                        'volume': total_volume,
                        'funds_count': len(sector_funds)
                    })
        
        # Сортируем по силе потока
        inflow_sectors = sorted(inflow_sectors, key=lambda x: x['net_flow'], reverse=True)
        outflow_sectors = sorted(outflow_sectors, key=lambda x: x['net_flow'], reverse=True)
        
        return {
            'inflow_sectors': inflow_sectors[:5],  # Топ-5 секторов с притоком
            'outflow_sectors': outflow_sectors[:5],  # Топ-5 секторов с оттоком
            'rotation_strength': len(inflow_sectors) + len(outflow_sectors),
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def get_detailed_fund_info(self) -> pd.DataFrame:
        """Получает детальную информацию о составах фондов"""
        
        detailed_info = []
        
        for _, row in self.etf_data.iterrows():
            ticker = row['ticker']
            isin = row.get('isin', '')
            
            # Получаем детальную информацию из базы данных
            fund_info = get_fund_category(isin)
            
            detailed_info.append({
                'ticker': ticker,
                'isin': isin,
                'full_name': row.get('full_name', ''),
                'category': fund_info['category'],
                'subcategory': fund_info['subcategory'],
                'risk_level': fund_info['risk_level'],
                'investment_style': fund_info['investment_style'],
                'avg_daily_volume': row.get('avg_daily_volume', 0),
                'avg_daily_value_rub': row.get('avg_daily_value_rub', 0),
                'current_price': row.get('current_price', 0),
                'last_price': row.get('last_price', 0),
                'annual_return': row.get('annual_return', 0),
                'volatility': row.get('volatility', 0),
                'sharpe_ratio': row.get('sharpe_ratio', 0),
                'has_detailed_info': fund_info['category'] != 'Неизвестно'
            })
        
        return pd.DataFrame(detailed_info)
    
    def analyze_composition_flows(self) -> Dict[str, any]:
        """Анализирует потоки на основе детальных составов фондов"""
        
        detailed_funds = self.get_detailed_fund_info()
        
        # Группируем по основным категориям
        category_flows = detailed_funds.groupby('category').agg({
            'avg_daily_volume': 'sum',
            'annual_return': 'mean',
            'volatility': 'mean',
            'ticker': 'count'
        }).round(2)
        
        # Анализируем потоки по стилям инвестирования
        style_flows = detailed_funds.groupby('investment_style').agg({
            'avg_daily_volume': 'sum',
            'annual_return': 'mean',
            'ticker': 'count'
        }).round(2)
        
        # Анализируем потоки по уровню риска
        risk_flows = detailed_funds.groupby('risk_level').agg({
            'avg_daily_volume': 'sum',
            'annual_return': 'mean',
            'ticker': 'count'
        }).round(2)
        
        # Статистика по детализации
        total_funds = len(detailed_funds)
        detailed_funds_count = len(detailed_funds[detailed_funds['has_detailed_info']])
        coverage = (detailed_funds_count / total_funds * 100) if total_funds > 0 else 0
        
        return {
            'category_flows': category_flows.to_dict('index'),
            'style_flows': style_flows.to_dict('index'),
            'risk_flows': risk_flows.to_dict('index'),
            'coverage_stats': {
                'total_funds': total_funds,
                'detailed_funds': detailed_funds_count,
                'coverage_percent': round(coverage, 1)
            },
            'analysis_timestamp': datetime.now().isoformat()
        }

def main():
    """Тестирование анализатора перетоков капитала"""
    
    # Загружаем данные ETF
    try:
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
        analyzer = CapitalFlowAnalyzer(etf_data)
        
        # Анализируем потоки капитала
        print("\n🔄 Анализ перетоков капитала...")
        
        sector_flows = analyzer.calculate_sector_flows()
        print(f"\n📈 Потоки капитала по секторам:")
        print(sector_flows[['volume_share', 'market_cap_share', 'avg_return', 'etf_count']])
        
        sentiment = analyzer.detect_risk_sentiment()
        print(f"\n🎯 Рыночные настроения: {sentiment['sentiment']} ({sentiment['confidence']}%)")
        print(f"   Защитные активы: {sentiment['defensive_share']}%")
        print(f"   Рисковые активы: {sentiment['risky_share']}%")
        
        momentum = analyzer.analyze_sector_momentum()
        print(f"\n⚡ Топ-3 сектора по моментуму:")
        for sector in momentum.head(3).index:
            data = momentum.loc[sector]
            print(f"   {sector}: {data['momentum_score']} ({data['momentum_trend']})")
            
        anomalies = analyzer.detect_flow_anomalies()
        if anomalies:
            print(f"\n⚠️  Обнаружено {len(anomalies)} аномалий:")
            for anomaly in anomalies[:3]:
                print(f"   {anomaly['type']} в секторе {anomaly['sector']}: {anomaly['value']} ({anomaly['severity']})")
        
        insights = analyzer.generate_flow_insights()
        print(f"\n💡 Общие инсайты:")
        print(f"   Лидеры по объему: {', '.join(insights['top_volume_sectors'])}")
        print(f"   Лидеры по моментуму: {', '.join(insights['momentum_leaders'])}")
        print(f"   Критических аномалий: {insights['critical_anomalies']}")
        
        print(f"\n✅ Анализ перетоков капитала завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")

if __name__ == "__main__":
    main()