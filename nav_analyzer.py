#!/usr/bin/env python3
"""
Анализатор СЧА (Стоимости Чистых Активов) БПИФ
Отслеживает размеры фондов, их рост/сокращение и концентрацию рынка
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import requests
import json
from pathlib import Path
import logging
from dataclasses import dataclass

@dataclass
class NAVAnalysisResult:
    """Результат анализа СЧА"""
    total_market_size: float
    largest_funds: List[Dict[str, any]]
    market_concentration: Dict[str, float]
    size_distribution: Dict[str, int]
    growth_leaders: List[Dict[str, any]]
    declining_funds: List[Dict[str, any]]

class NAVAnalyzer:
    """Анализатор СЧА (стоимости чистых активов) БПИФ"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self._cache = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('NAVAnalyzer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def get_fund_nav_from_moex(self, ticker: str) -> Dict[str, any]:
        """
        Получает данные о СЧА фонда из MOEX API
        
        Args:
            ticker: Тикер фонда
            
        Returns:
            Данные о СЧА и капитализации
        """
        try:
            # Основная информация о фонде
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                nav_info = {}
                
                # Извлекаем данные из marketdata
                if 'marketdata' in data and 'data' in data['marketdata']:
                    columns = data['marketdata']['columns']
                    market_data = data['marketdata']['data']
                    
                    if market_data:
                        row = market_data[0]
                        market_info = dict(zip(columns, row))
                        
                        nav_info.update({
                            'last_price': market_info.get('LAST', 0),
                            'market_cap': market_info.get('MARKETPRICE', 0),
                            'volume': market_info.get('VOLTODAY', 0),
                            'value': market_info.get('VALTODAY', 0),
                            'change': market_info.get('CHANGE', 0),
                            'change_percent': market_info.get('PRCCHANGE', 0),
                        })
                
                # Извлекаем данные из securities
                if 'securities' in data and 'data' in data['securities']:
                    columns = data['securities']['columns']
                    securities_data = data['securities']['data']
                    
                    if securities_data:
                        row = securities_data[0]
                        security_info = dict(zip(columns, row))
                        
                        nav_info.update({
                            'full_name': security_info.get('SECNAME', ''),
                            'short_name': security_info.get('SHORTNAME', ''),
                            'isin': security_info.get('ISIN', ''),
                            'lot_size': security_info.get('LOTSIZE', 1),
                            'face_value': security_info.get('FACEVALUE', 0),
                            'issue_size': security_info.get('ISSUESIZE', 0),
                        })
                
                # Рассчитываем приблизительную СЧА
                if nav_info.get('last_price') and nav_info.get('issue_size'):
                    estimated_nav = nav_info['last_price'] * nav_info['issue_size']
                    nav_info['estimated_nav'] = estimated_nav
                
                return nav_info
            else:
                self.logger.warning(f"MOEX API error for {ticker}: HTTP {response.status_code}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting NAV data for {ticker}: {e}")
            return {}
    
    def analyze_fund_sizes(self, etf_data: pd.DataFrame) -> NAVAnalysisResult:
        """
        Анализирует размеры фондов и их распределение
        
        Args:
            etf_data: Данные о фондах
            
        Returns:
            Результат анализа СЧА
        """
        self.logger.info("Анализ размеров фондов...")
        
        # Подготавливаем данные для анализа
        funds_with_size = []
        
        for _, fund in etf_data.iterrows():
            ticker = fund['ticker']
            
            # Используем market_cap из данных или рассчитываем
            market_cap = fund.get('market_cap', 0)
            
            # Если market_cap нет или равен 0, пытаемся рассчитать
            if not market_cap or market_cap == 0:
                last_price = fund.get('last_price', fund.get('current_price', 0))
                avg_volume = fund.get('avg_daily_volume', 0)
                
                # Приблизительная оценка на основе оборотов
                if last_price and avg_volume:
                    # Предполагаем, что дневной оборот составляет 1-2% от СЧА
                    estimated_nav = (avg_volume * last_price) * 50  # Консервативная оценка
                    market_cap = estimated_nav
            
            fund_info = {
                'ticker': ticker,
                'name': fund.get('full_name', fund.get('short_name', ticker)),
                'market_cap': market_cap,
                'last_price': fund.get('last_price', fund.get('current_price', 0)),
                'volume': fund.get('avg_daily_volume', 0),
                'annual_return': fund.get('annual_return', 0),
                'volatility': fund.get('volatility', 0),
                'inception_date': fund.get('inception_date', ''),
                'category': fund.get('category', 'Неизвестно')
            }
            
            funds_with_size.append(fund_info)
        
        funds_df = pd.DataFrame(funds_with_size)
        
        # Фильтруем фонды с валидными данными о размере
        valid_funds = funds_df[funds_df['market_cap'] > 0].copy()
        
        if valid_funds.empty:
            self.logger.warning("Нет фондов с валидными данными о размере")
            return NAVAnalysisResult(
                total_market_size=0,
                largest_funds=[],
                market_concentration={},
                size_distribution={},
                growth_leaders=[],
                declining_funds=[]
            )
        
        # Общий размер рынка
        total_market_size = valid_funds['market_cap'].sum()
        
        # Крупнейшие фонды
        largest_funds = valid_funds.nlargest(20, 'market_cap').to_dict('records')
        
        # Концентрация рынка
        market_concentration = self._calculate_market_concentration(valid_funds)
        
        # Распределение по размерам
        size_distribution = self._categorize_by_size(valid_funds)
        
        # Лидеры роста и сокращения (на основе доходности как прокси)
        growth_leaders = valid_funds.nlargest(10, 'annual_return').to_dict('records')
        declining_funds = valid_funds.nsmallest(10, 'annual_return').to_dict('records')
        
        return NAVAnalysisResult(
            total_market_size=total_market_size,
            largest_funds=largest_funds,
            market_concentration=market_concentration,
            size_distribution=size_distribution,
            growth_leaders=growth_leaders,
            declining_funds=declining_funds
        )
    
    def _calculate_market_concentration(self, funds_df: pd.DataFrame) -> Dict[str, float]:
        """Рассчитывает показатели концентрации рынка"""
        
        total_cap = funds_df['market_cap'].sum()
        funds_sorted = funds_df.sort_values('market_cap', ascending=False)
        
        # Доли крупнейших фондов
        top_5_share = funds_sorted.head(5)['market_cap'].sum() / total_cap * 100
        top_10_share = funds_sorted.head(10)['market_cap'].sum() / total_cap * 100
        top_20_share = funds_sorted.head(20)['market_cap'].sum() / total_cap * 100
        
        # Индекс Херфиндаля-Хиршмана
        market_shares = funds_df['market_cap'] / total_cap
        hhi = (market_shares ** 2).sum() * 10000  # Умножаем на 10000 для стандартного формата
        
        # Классификация концентрации
        if hhi < 1000:
            concentration_level = "Низкая концентрация"
        elif hhi < 1800:
            concentration_level = "Умеренная концентрация"
        else:
            concentration_level = "Высокая концентрация"
        
        return {
            'top_5_share': round(top_5_share, 1),
            'top_10_share': round(top_10_share, 1),
            'top_20_share': round(top_20_share, 1),
            'hhi_index': round(hhi, 0),
            'concentration_level': concentration_level,
            'total_funds': len(funds_df)
        }
    
    def _categorize_by_size(self, funds_df: pd.DataFrame) -> Dict[str, int]:
        """Категоризирует фонды по размеру"""
        
        # Определяем пороги (в рублях)
        mega_threshold = 10_000_000_000    # 10 млрд
        large_threshold = 1_000_000_000    # 1 млрд  
        medium_threshold = 100_000_000     # 100 млн
        small_threshold = 10_000_000       # 10 млн
        
        size_categories = {
            'mega_funds': len(funds_df[funds_df['market_cap'] >= mega_threshold]),
            'large_funds': len(funds_df[
                (funds_df['market_cap'] >= large_threshold) & 
                (funds_df['market_cap'] < mega_threshold)
            ]),
            'medium_funds': len(funds_df[
                (funds_df['market_cap'] >= medium_threshold) & 
                (funds_df['market_cap'] < large_threshold)
            ]),
            'small_funds': len(funds_df[
                (funds_df['market_cap'] >= small_threshold) & 
                (funds_df['market_cap'] < medium_threshold)
            ]),
            'micro_funds': len(funds_df[funds_df['market_cap'] < small_threshold]),
        }
        
        return size_categories
    
    def analyze_size_trends(self, current_data: pd.DataFrame, 
                           historical_periods: List[Tuple[datetime, datetime]]) -> Dict[str, any]:
        """
        Анализирует тренды изменения размеров фондов
        
        Args:
            current_data: Текущие данные
            historical_periods: Список исторических периодов для сравнения
            
        Returns:
            Анализ трендов размеров
        """
        self.logger.info("Анализ трендов размеров фондов...")
        
        # Текущий анализ
        current_analysis = self.analyze_fund_sizes(current_data)
        
        # Анализ по категориям
        category_analysis = self._analyze_by_categories(current_data)
        
        # Анализ возраста фондов
        age_analysis = self._analyze_by_age(current_data)
        
        return {
            'current_snapshot': {
                'total_market_size': current_analysis.total_market_size,
                'concentration': current_analysis.market_concentration,
                'size_distribution': current_analysis.size_distribution
            },
            'category_breakdown': category_analysis,
            'age_analysis': age_analysis,
            'largest_funds': current_analysis.largest_funds[:10],
            'fastest_growing': current_analysis.growth_leaders[:5],
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def _analyze_by_categories(self, funds_df: pd.DataFrame) -> Dict[str, any]:
        """Анализирует размеры по категориям фондов"""
        
        category_stats = funds_df.groupby('category').agg({
            'market_cap': ['sum', 'mean', 'count'],
            'annual_return': 'mean',
            'volatility': 'mean'
        }).round(2)
        
        # Упрощаем структуру колонок
        category_stats.columns = ['total_cap', 'avg_cap', 'funds_count', 'avg_return', 'avg_volatility']
        
        # Добавляем долю рынка
        total_market = category_stats['total_cap'].sum()
        category_stats['market_share'] = (category_stats['total_cap'] / total_market * 100).round(1)
        
        return category_stats.to_dict('index')
    
    def _analyze_by_age(self, funds_df: pd.DataFrame) -> Dict[str, any]:
        """Анализирует размеры по возрасту фондов"""
        
        # Рассчитываем возраст фондов
        current_date = datetime.now()
        
        funds_with_age = funds_df.copy()
        funds_with_age['inception_date'] = pd.to_datetime(funds_with_age['inception_date'], errors='coerce')
        funds_with_age = funds_with_age.dropna(subset=['inception_date'])
        
        if funds_with_age.empty:
            return {}
        
        funds_with_age['age_years'] = (
            current_date - funds_with_age['inception_date']
        ).dt.days / 365.25
        
        # Категоризируем по возрасту
        def categorize_age(age):
            if age < 1:
                return 'new_funds'  # Новые (до года)
            elif age < 3:
                return 'young_funds'  # Молодые (1-3 года)
            elif age < 5:
                return 'mature_funds'  # Зрелые (3-5 лет)
            else:
                return 'veteran_funds'  # Ветераны (5+ лет)
        
        funds_with_age['age_category'] = funds_with_age['age_years'].apply(categorize_age)
        
        age_stats = funds_with_age.groupby('age_category').agg({
            'market_cap': ['sum', 'mean', 'count'],
            'annual_return': 'mean',
            'age_years': 'mean'
        }).round(2)
        
        age_stats.columns = ['total_cap', 'avg_cap', 'funds_count', 'avg_return', 'avg_age']
        
        return age_stats.to_dict('index')
    
    def generate_nav_insights(self, analysis_result: NAVAnalysisResult) -> List[str]:
        """Генерирует инсайты на основе анализа СЧА"""
        
        insights = []
        
        # Общий размер рынка
        total_size_bil = analysis_result.total_market_size / 1_000_000_000
        insights.append(f"Общий размер рынка БПИФ: {total_size_bil:.1f} млрд руб.")
        
        # Концентрация
        concentration = analysis_result.market_concentration
        insights.append(f"Топ-5 фондов контролируют {concentration['top_5_share']}% рынка")
        insights.append(f"Уровень концентрации: {concentration['concentration_level']}")
        
        # Размерное распределение
        size_dist = analysis_result.size_distribution
        total_funds = sum(size_dist.values())
        
        if size_dist['mega_funds'] > 0:
            insights.append(f"Мега-фондов (>10 млрд): {size_dist['mega_funds']}")
        
        insights.append(f"Крупных фондов (>1 млрд): {size_dist['large_funds']}")
        insights.append(f"Средних фондов (100млн-1млрд): {size_dist['medium_funds']}")
        
        # Крупнейший фонд
        if analysis_result.largest_funds:
            largest = analysis_result.largest_funds[0]
            largest_size_bil = largest['market_cap'] / 1_000_000_000
            insights.append(f"Крупнейший фонд: {largest['ticker']} ({largest_size_bil:.1f} млрд руб.)")
        
        return insights

def main():
    """Тестирование анализатора СЧА"""
    
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
        analyzer = NAVAnalyzer()
        
        # Анализируем размеры фондов
        print("\n💰 Анализ размеров фондов (СЧА)...")
        
        analysis_result = analyzer.analyze_fund_sizes(etf_data)
        
        # Выводим основные результаты
        print(f"\n📈 Общий размер рынка: {analysis_result.total_market_size / 1_000_000_000:.1f} млрд руб.")
        
        print(f"\n🏢 Концентрация рынка:")
        conc = analysis_result.market_concentration
        print(f"   Топ-5 фондов: {conc['top_5_share']}% рынка")
        print(f"   Топ-10 фондов: {conc['top_10_share']}% рынка")
        print(f"   Индекс HHI: {conc['hhi_index']}")
        print(f"   Уровень: {conc['concentration_level']}")
        
        print(f"\n📊 Распределение по размерам:")
        size_dist = analysis_result.size_distribution
        print(f"   Мега-фонды (>10млрд): {size_dist['mega_funds']}")
        print(f"   Крупные (1-10млрд): {size_dist['large_funds']}")
        print(f"   Средние (100млн-1млрд): {size_dist['medium_funds']}")
        print(f"   Малые (10-100млн): {size_dist['small_funds']}")
        print(f"   Микро (<10млн): {size_dist['micro_funds']}")
        
        print(f"\n🏆 Топ-10 крупнейших фондов:")
        for i, fund in enumerate(analysis_result.largest_funds[:10], 1):
            size_bil = fund['market_cap'] / 1_000_000_000
            print(f"   {i:2d}. {fund['ticker']}: {size_bil:.2f} млрд руб. ({fund['annual_return']:+.1f}%)")
        
        print(f"\n📈 Топ-5 лидеров роста:")
        for i, fund in enumerate(analysis_result.growth_leaders[:5], 1):
            size_mil = fund['market_cap'] / 1_000_000
            print(f"   {i}. {fund['ticker']}: {fund['annual_return']:+.1f}% ({size_mil:.0f} млн руб.)")
        
        # Генерируем инсайты
        insights = analyzer.generate_nav_insights(analysis_result)
        print(f"\n💡 Ключевые инсайты:")
        for insight in insights:
            print(f"   • {insight}")
        
        # Анализ трендов
        print(f"\n🔍 Анализ трендов...")
        trends = analyzer.analyze_size_trends(etf_data, [])
        
        if 'category_breakdown' in trends and trends['category_breakdown']:
            print(f"\n🏷️ Топ-3 категории по размеру:")
            category_data = trends['category_breakdown']
            sorted_categories = sorted(
                category_data.items(), 
                key=lambda x: x[1].get('total_cap', 0), 
                reverse=True
            )
            
            for i, (category, data) in enumerate(sorted_categories[:3], 1):
                total_bil = data.get('total_cap', 0) / 1_000_000_000
                funds_count = data.get('funds_count', 0)
                market_share = data.get('market_share', 0)
                print(f"   {i}. {category}: {total_bil:.1f} млрд руб. ({funds_count} фондов, {market_share:.1f}%)")
        
        print(f"\n✅ Анализ СЧА завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка анализа СЧА: {e}")

if __name__ == "__main__":
    main()