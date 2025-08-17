"""
Датасет с информацией о российских биржевых паевых инвестиционных фондах
"""

import pandas as pd
import numpy as np
from datetime import datetime

class RussianETFDataset:
    
    def __init__(self):
        self.etf_data = self.create_comprehensive_dataset()
        
    def create_comprehensive_dataset(self):
        """
        Создание комплексного датасета российских БПИФов
        """
        
        # Основные российские БПИФы (данные приблизительные для демонстрации структуры)
        etf_data = [
            # Сбер Управление Активами
            {
                'ticker': 'SBMX',
                'name': 'Сбер - Мосбиржа',
                'management_company': 'Сбер Управление Активами',
                'category': 'Акции',
                'subcategory': 'Российские акции',
                'geography': 'Россия',
                'inception_date': '2021-01-15',
                'assets_under_management_mln_rub': 15000,
                'expense_ratio': 0.95,
                'performance_1m': 2.5,
                'performance_3m': 8.2,
                'performance_6m': 15.1,
                'performance_1y': 12.8,
                'performance_ytd': 18.5,
                'volatility_1y': 25.6,
                'sharpe_ratio': 0.45,
                'max_drawdown': -18.2,
                'liquidity_rating': 'Высокая',
                'tracking_index': 'Индекс МосБиржи'
            },
            {
                'ticker': 'SBRB',
                'name': 'Сбер - Облигации',
                'management_company': 'Сбер Управление Активами',
                'category': 'Облигации',
                'subcategory': 'Государственные облигации',
                'geography': 'Россия',
                'inception_date': '2020-08-20',
                'assets_under_management_mln_rub': 8500,
                'expense_ratio': 0.45,
                'performance_1m': 1.2,
                'performance_3m': 3.8,
                'performance_6m': 7.5,
                'performance_1y': 8.9,
                'performance_ytd': 9.2,
                'volatility_1y': 8.2,
                'sharpe_ratio': 0.92,
                'max_drawdown': -4.5,
                'liquidity_rating': 'Высокая',
                'tracking_index': 'Индекс государственных облигаций'
            },
            
            # ВТБ Капитал
            {
                'ticker': 'VTBX',
                'name': 'ВТБ - Мосбиржа',
                'management_company': 'ВТБ Капитал Управление Активами',
                'category': 'Акции',
                'subcategory': 'Российские акции',
                'geography': 'Россия',
                'inception_date': '2020-12-10',
                'assets_under_management_mln_rub': 12000,
                'expense_ratio': 0.89,
                'performance_1m': 2.1,
                'performance_3m': 7.8,
                'performance_6m': 14.2,
                'performance_1y': 11.5,
                'performance_ytd': 17.8,
                'volatility_1y': 24.8,
                'sharpe_ratio': 0.42,
                'max_drawdown': -19.1,
                'liquidity_rating': 'Высокая',
                'tracking_index': 'Индекс МосБиржи'
            },
            {
                'ticker': 'VTBE',
                'name': 'ВТБ - Европа',
                'management_company': 'ВТБ Капитал Управление Активами',
                'category': 'Акции',
                'subcategory': 'Зарубежные акции',
                'geography': 'Европа',
                'inception_date': '2021-06-15',
                'assets_under_management_mln_rub': 3200,
                'expense_ratio': 1.25,
                'performance_1m': -1.2,
                'performance_3m': 4.5,
                'performance_6m': 8.9,
                'performance_1y': 6.2,
                'performance_ytd': 12.1,
                'volatility_1y': 22.1,
                'sharpe_ratio': 0.28,
                'max_drawdown': -15.6,
                'liquidity_rating': 'Средняя',
                'tracking_index': 'MSCI Europe'
            },
            
            # Тинькофф Капитал
            {
                'ticker': 'TECH',
                'name': 'Тинькофф - Технологии',
                'management_company': 'Тинькофф Капитал',
                'category': 'Акции',
                'subcategory': 'Технологические акции',
                'geography': 'США',
                'inception_date': '2021-03-20',
                'assets_under_management_mln_rub': 5800,
                'expense_ratio': 1.15,
                'performance_1m': 3.8,
                'performance_3m': 12.5,
                'performance_6m': 22.1,
                'performance_1y': 18.9,
                'performance_ytd': 28.2,
                'volatility_1y': 28.5,
                'sharpe_ratio': 0.62,
                'max_drawdown': -22.8,
                'liquidity_rating': 'Средняя',
                'tracking_index': 'NASDAQ-100'
            },
            {
                'ticker': 'TGLD',
                'name': 'Тинькофф - Золото',
                'management_company': 'Тинькофф Капитал',
                'category': 'Товарные',
                'subcategory': 'Драгоценные металлы',
                'geography': 'Глобальный',
                'inception_date': '2021-09-10',
                'assets_under_management_mln_rub': 2100,
                'expense_ratio': 0.85,
                'performance_1m': -0.5,
                'performance_3m': 2.1,
                'performance_6m': 8.8,
                'performance_1y': 15.2,
                'performance_ytd': 11.5,
                'volatility_1y': 18.5,
                'sharpe_ratio': 0.75,
                'max_drawdown': -12.3,
                'liquidity_rating': 'Средняя',
                'tracking_index': 'Gold Spot Price'
            },
            
            # Альфа-Капитал
            {
                'ticker': 'ALFA',
                'name': 'Альфа - Дивиденды',
                'management_company': 'Альфа-Капитал',
                'category': 'Акции',
                'subcategory': 'Дивидендные акции',
                'geography': 'Россия',
                'inception_date': '2020-05-15',
                'assets_under_management_mln_rub': 4200,
                'expense_ratio': 1.05,
                'performance_1m': 1.8,
                'performance_3m': 6.2,
                'performance_6m': 11.5,
                'performance_1y': 14.8,
                'performance_ytd': 16.2,
                'volatility_1y': 21.2,
                'sharpe_ratio': 0.68,
                'max_drawdown': -14.5,
                'liquidity_rating': 'Средняя',
                'tracking_index': 'Индекс дивидендных акций'
            },
            
            # Газпромбанк
            {
                'ticker': 'GPBR',
                'name': 'ГПБ - Рубль',
                'management_company': 'Газпромбанк - Управление активами',
                'category': 'Денежный рынок',
                'subcategory': 'Краткосрочные инструменты',
                'geography': 'Россия',
                'inception_date': '2019-11-20',
                'assets_under_management_mln_rub': 6800,
                'expense_ratio': 0.35,
                'performance_1m': 0.8,
                'performance_3m': 2.5,
                'performance_6m': 5.2,
                'performance_1y': 7.8,
                'performance_ytd': 6.5,
                'volatility_1y': 2.1,
                'sharpe_ratio': 3.25,
                'max_drawdown': -0.8,
                'liquidity_rating': 'Очень высокая',
                'tracking_index': 'Ключевая ставка ЦБ РФ'
            },
            
            # Райффайзенбанк
            {
                'ticker': 'RAIF',
                'name': 'Райффайзен - Баланс',
                'management_company': 'УК Райффайзенбанк',
                'category': 'Смешанные',
                'subcategory': 'Консервативные',
                'geography': 'Россия',
                'inception_date': '2020-02-10',
                'assets_under_management_mln_rub': 3500,
                'expense_ratio': 1.35,
                'performance_1m': 1.5,
                'performance_3m': 4.8,
                'performance_6m': 9.2,
                'performance_1y': 10.5,
                'performance_ytd': 12.8,
                'volatility_1y': 12.5,
                'sharpe_ratio': 0.78,
                'max_drawdown': -8.2,
                'liquidity_rating': 'Высокая',
                'tracking_index': 'Смешанный индекс 60/40'
            },
            
            # Открытие Брокер
            {
                'ticker': 'OPEN',
                'name': 'Открытие - ММВБ',
                'management_company': 'Открытие Брокер',
                'category': 'Акции',
                'subcategory': 'Российские акции',
                'geography': 'Россия',
                'inception_date': '2021-07-01',
                'assets_under_management_mln_rub': 2800,
                'expense_ratio': 0.95,
                'performance_1m': 2.2,
                'performance_3m': 7.5,
                'performance_6m': 13.8,
                'performance_1y': 10.8,
                'performance_ytd': 16.5,
                'volatility_1y': 26.2,
                'sharpe_ratio': 0.38,
                'max_drawdown': -20.1,
                'liquidity_rating': 'Средняя',
                'tracking_index': 'Индекс ММВБ'
            }
        ]
        
        return pd.DataFrame(etf_data)
    
    def get_management_companies_summary(self):
        """
        Сводка по управляющим компаниям
        """
        summary = self.etf_data.groupby('management_company').agg({
            'ticker': 'count',
            'assets_under_management_mln_rub': 'sum',
            'expense_ratio': 'mean',
            'performance_1y': 'mean',
            'volatility_1y': 'mean'
        }).round(2)
        
        summary.columns = ['Количество фондов', 'Активы под управлением (млн руб)', 
                          'Средняя комиссия (%)', 'Средняя доходность 1г (%)', 
                          'Средняя волатильность (%)']
        
        # Расчет доли рынка
        total_assets = summary['Активы под управлением (млн руб)'].sum()
        summary['Доля рынка (%)'] = (summary['Активы под управлением (млн руб)'] / total_assets * 100).round(1)
        
        return summary.sort_values('Активы под управлением (млн руб)', ascending=False)
    
    def get_category_analysis(self):
        """
        Анализ по категориям фондов
        """
        category_summary = self.etf_data.groupby('category').agg({
            'ticker': 'count',
            'assets_under_management_mln_rub': 'sum',
            'performance_1y': 'mean',
            'volatility_1y': 'mean',
            'sharpe_ratio': 'mean'
        }).round(2)
        
        category_summary.columns = ['Количество фондов', 'Активы (млн руб)', 
                                  'Доходность 1г (%)', 'Волатильность (%)', 'Коэф. Шарпа']
        
        return category_summary.sort_values('Активы (млн руб)', ascending=False)
    
    def get_performance_leaders(self, metric='performance_1y', top_n=5):
        """
        Лидеры по показателям доходности
        """
        leaders = self.etf_data.nlargest(top_n, metric)[
            ['ticker', 'name', 'management_company', 'category', metric]
        ].round(2)
        
        return leaders
    
    def save_dataset(self, filename='russian_etf_data.csv'):
        """
        Сохранение датасета в CSV файл
        """
        filepath = f'/Users/tumowuh/Desktop/market analysis/{filename}'
        self.etf_data.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Датасет сохранен в файл: {filepath}")
        return filepath

if __name__ == "__main__":
    dataset = RussianETFDataset()
    
    print("=== АНАЛИЗ РОССИЙСКИХ БПИФ ===\n")
    
    print("1. СВОДКА ПО УПРАВЛЯЮЩИМ КОМПАНИЯМ:")
    print(dataset.get_management_companies_summary())
    print("\n")
    
    print("2. АНАЛИЗ ПО КАТЕГОРИЯМ:")
    print(dataset.get_category_analysis())
    print("\n")
    
    print("3. ЛИДЕРЫ ПО ДОХОДНОСТИ (1 год):")
    print(dataset.get_performance_leaders('performance_1y'))
    print("\n")
    
    print("4. ЛУЧШИЕ ПО КОЭФФИЦИЕНТУ ШАРПА:")
    print(dataset.get_performance_leaders('sharpe_ratio'))
    print("\n")
    
    # Сохранение данных
    dataset.save_dataset()