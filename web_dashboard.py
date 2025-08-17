#!/usr/bin/env python3
"""
Веб-дашборд для анализа ETF на Flask
Запускается на localhost для удобного просмотра аналитики
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.utils
import json
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Импортируем наши модули
from risk_metrics import RiskMetricsCalculator
from correlation_analysis import CorrelationAnalyzer
from portfolio_optimization import PortfolioOptimizer

app = Flask(__name__)

class ETFWebDashboard:
    """Веб-дашборд для анализа ETF"""
    
    def __init__(self):
        self.df = None
        self.enhanced_df = None
        self.risk_calculator = RiskMetricsCalculator()
        self.correlation_analyzer = CorrelationAnalyzer()
        self.portfolio_optimizer = PortfolioOptimizer()
        self.load_data()
    
    def load_data(self):
        """Загружает данные ETF"""
        try:
            # Ищем последние файлы
            data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
            if not data_files:
                data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
            
            if not data_files:
                print("❌ Файлы с данными ETF не найдены")
                return False
            
            latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
            print(f"📊 Загружаем данные из {latest_data}")
            
            self.df = pd.read_csv(latest_data)
            
            # Обогащаем данные метриками если нужно
            if 'sharpe_ratio' not in self.df.columns:
                self.enhance_data()
            else:
                self.enhanced_df = self.df.copy()
            
            print(f"✅ Загружено {len(self.df)} ETF")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка загрузки данных: {e}")
            return False
    
    def enhance_data(self):
        """Обогащает данные расчетными метриками"""
        if self.df is None:
            return
        
        print("🔄 Рассчитываем дополнительные метрики...")
        
        enhanced_df = self.df.copy()
        
        # Рассчитываем Sharpe ratio (безрисковая ставка 15%)
        risk_free_rate = 15.0
        enhanced_df['sharpe_ratio'] = (enhanced_df['annual_return'] - risk_free_rate) / enhanced_df['volatility']
        
        # Рассчитываем Sortino ratio (упрощенная версия)
        enhanced_df['sortino_ratio'] = enhanced_df['annual_return'] / (enhanced_df['volatility'] * 0.7)
        
        # Рассчитываем альфу и бету
        market_return = enhanced_df['annual_return'].mean()
        market_volatility = enhanced_df['volatility'].mean()
        
        enhanced_df['beta'] = enhanced_df['volatility'] / market_volatility
        enhanced_df['alpha'] = enhanced_df['annual_return'] - (risk_free_rate + enhanced_df['beta'] * (market_return - risk_free_rate))
        
        # Категоризация по риску
        enhanced_df['risk_level'] = pd.cut(
            enhanced_df['volatility'], 
            bins=[0, 10, 20, float('inf')],
            labels=['Низкий', 'Средний', 'Высокий']
        )
        
        # Категоризация по доходности
        enhanced_df['return_tier'] = pd.cut(
            enhanced_df['annual_return'], 
            bins=[-float('inf'), 5, 15, float('inf')],
            labels=['Низкая', 'Средняя', 'Высокая']
        )
        
        self.enhanced_df = enhanced_df
        print(f"✅ Данные обогащены метриками для {len(enhanced_df)} ETF")
    
    def get_summary_stats(self) -> Dict:
        """Возвращает сводную статистику"""
        if self.enhanced_df is None:
            return {}
        
        return {
            'total_etfs': len(self.enhanced_df),
            'avg_return': round(self.enhanced_df['annual_return'].mean(), 1),
            'avg_volatility': round(self.enhanced_df['volatility'].mean(), 1),
            'avg_sharpe': round(self.enhanced_df['sharpe_ratio'].mean(), 2),
            'categories': len(self.enhanced_df['category'].unique()),
            'best_etf': {
                'ticker': self.enhanced_df.loc[self.enhanced_df['annual_return'].idxmax(), 'ticker'],
                'return': round(self.enhanced_df['annual_return'].max(), 1)
            },
            'best_sharpe': {
                'ticker': self.enhanced_df.loc[self.enhanced_df['sharpe_ratio'].idxmax(), 'ticker'],
                'sharpe': round(self.enhanced_df['sharpe_ratio'].max(), 2)
            }
        }
    
    def create_risk_return_plot(self) -> str:
        """Создает график риск-доходность"""
        if self.enhanced_df is None:
            return "{}"
        
        valid_data = self.enhanced_df[(self.enhanced_df['annual_return'].notna()) & 
                                     (self.enhanced_df['volatility'].notna()) & 
                                     (self.enhanced_df['avg_daily_volume'].notna())]
        
        fig = px.scatter(
            valid_data, 
            x='volatility', 
            y='annual_return',
            size='avg_daily_volume',
            color='category',
            hover_name='ticker',
            hover_data=['sharpe_ratio', 'management_company'],
            title='📊 Анализ Риск-Доходность ETF',
            labels={
                'volatility': 'Волатильность (%)',
                'annual_return': 'Годовая доходность (%)',
                'avg_daily_volume': 'Средний объем торгов'
            }
        )
        
        # Добавляем безрисковую ставку
        fig.add_hline(y=15, line_dash="dot", line_color="green", 
                     annotation_text="Безрисковая ставка (15%)")
        
        fig.update_layout(height=600)
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_sector_analysis_plot(self) -> str:
        """Создает анализ по секторам"""
        if self.enhanced_df is None:
            return "{}"
        
        # Группируем по категориям
        sector_stats = self.enhanced_df.groupby('category').agg({
            'annual_return': 'mean',
            'volatility': 'mean',
            'sharpe_ratio': 'mean',
            'ticker': 'count'
        }).round(2)
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Средняя доходность', 'Средняя волатильность',
                           'Средний Sharpe Ratio', 'Количество ETF'),
            specs=[[{}, {}], [{}, {}]]
        )
        
        categories = sector_stats.index.tolist()
        
        # График 1: Доходность
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['annual_return'], 
                   name='Доходность (%)', marker_color='lightgreen'),
            row=1, col=1
        )
        
        # График 2: Волатильность
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['volatility'], 
                   name='Волатильность (%)', marker_color='lightcoral'),
            row=1, col=2
        )
        
        # График 3: Sharpe Ratio
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['sharpe_ratio'], 
                   name='Sharpe Ratio', marker_color='lightblue'),
            row=2, col=1
        )
        
        # График 4: Количество ETF
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['ticker'], 
                   name='Количество', marker_color='orange'),
            row=2, col=2
        )
        
        fig.update_layout(
            title='🏢 Секторальный анализ ETF',
            height=800,
            showlegend=False
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_performance_comparison_plot(self) -> str:
        """Создает сравнение доходности"""
        if self.enhanced_df is None:
            return "{}"
        
        # Сортируем по доходности
        df_sorted = self.enhanced_df.sort_values('annual_return', ascending=True)
        
        # Берем топ и аутсайдеров
        top_performers = df_sorted.tail(10)
        worst_performers = df_sorted.head(10)
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('🏆 ТОП-10 по доходности', '📉 Аутсайдеры'),
            horizontal_spacing=0.15
        )
        
        # Топ исполнители
        fig.add_trace(
            go.Bar(
                y=top_performers['ticker'],
                x=top_performers['annual_return'],
                orientation='h',
                name='Лидеры',
                marker_color='green',
                text=top_performers['annual_return'].round(1),
                textposition='outside'
            ),
            row=1, col=1
        )
        
        # Аутсайдеры
        fig.add_trace(
            go.Bar(
                y=worst_performers['ticker'],
                x=worst_performers['annual_return'],
                orientation='h',
                name='Аутсайдеры',
                marker_color='red',
                text=worst_performers['annual_return'].round(1),
                textposition='outside'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            title='📊 Сравнение доходности: лидеры vs аутсайдеры',
            height=600,
            showlegend=False
        )
        
        fig.update_xaxes(title_text="Годовая доходность (%)", row=1, col=1)
        fig.update_xaxes(title_text="Годовая доходность (%)", row=1, col=2)
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_correlation_heatmap(self) -> str:
        """Создает корреляционную тепловую карту"""
        if self.enhanced_df is None:
            return "{}"
        
        # Берем топ-15 ETF по объему для анализа корреляций
        top_etfs = self.enhanced_df.nlargest(15, 'avg_daily_volume')
        
        # Генерируем корреляционную матрицу
        correlation_matrix = self.correlation_analyzer.generate_synthetic_correlations(
            top_etfs.to_dict('records')
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=correlation_matrix.values,
            x=correlation_matrix.columns,
            y=correlation_matrix.index,
            colorscale='RdBu',
            zmid=0,
            text=correlation_matrix.round(2).values,
            texttemplate="%{text}",
            textfont={"size": 10},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title='🔗 Корреляционная матрица ТОП-15 ETF',
            height=600,
            xaxis_title='ETF',
            yaxis_title='ETF'
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_efficient_frontier_plot(self) -> str:
        """Создает эффективную границу"""
        if self.enhanced_df is None:
            return "{}"
        
        try:
            # Берем топ-10 ETF для оптимизации
            top_etfs = self.enhanced_df.nlargest(10, 'sharpe_ratio').to_dict('records')
            
            # Подготавливаем данные
            returns, cov_matrix, tickers = self.portfolio_optimizer.prepare_data(top_etfs)
            
            # Строим эффективную границу
            efficient_frontier = self.portfolio_optimizer.build_efficient_frontier(
                returns, cov_matrix, num_portfolios=20
            )
            
            # Находим оптимальные портфели
            optimal_portfolios = self.portfolio_optimizer.find_optimal_portfolios(top_etfs)
            
            # Создаем график
            fig = go.Figure()
            
            # Эффективная граница
            fig.add_trace(go.Scatter(
                x=[v * 100 for v in efficient_frontier['volatilities']],
                y=[r * 100 for r in efficient_frontier['returns']],
                mode='lines+markers',
                name='Эффективная граница',
                line=dict(color='blue', width=3),
                marker=dict(size=6)
            ))
            
            # Оптимальные портфели
            colors = ['red', 'green', 'orange', 'purple']
            for i, (name, portfolio) in enumerate(optimal_portfolios.items()):
                fig.add_trace(go.Scatter(
                    x=[portfolio['volatility']],
                    y=[portfolio['return']],
                    mode='markers',
                    name=portfolio['description'],
                    marker=dict(size=15, color=colors[i % len(colors)]),
                    text=f"Sharpe: {portfolio['sharpe_ratio']:.2f}",
                    hovertemplate=f"{portfolio['description']}<br>" +
                                 f"Доходность: {portfolio['return']:.1f}%<br>" +
                                 f"Риск: {portfolio['volatility']:.1f}%<br>" +
                                 f"Sharpe: {portfolio['sharpe_ratio']:.2f}<extra></extra>"
                ))
            
            # Индивидуальные ETF
            individual_returns = [etf['annual_return'] for etf in top_etfs]
            individual_vols = [etf['volatility'] for etf in top_etfs]
            individual_tickers = [etf['ticker'] for etf in top_etfs]
            
            fig.add_trace(go.Scatter(
                x=individual_vols,
                y=individual_returns,
                mode='markers',
                name='Индивидуальные ETF',
                marker=dict(size=8, color='lightblue', opacity=0.7),
                text=individual_tickers,
                hovertemplate="%{text}<br>" +
                             "Доходность: %{y:.1f}%<br>" +
                             "Риск: %{x:.1f}%<extra></extra>"
            ))
            
            fig.update_layout(
                title='📈 Эффективная граница портфеля (ТОП-10 ETF)',
                xaxis_title='Волатильность (%)',
                yaxis_title='Ожидаемая доходность (%)',
                height=600,
                showlegend=True
            )
            
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
            
        except Exception as e:
            print(f"Ошибка создания эффективной границы: {e}")
            return "{}"
    
    def get_investment_recommendations(self) -> Dict:
        """Возвращает инвестиционные рекомендации"""
        if self.enhanced_df is None:
            return {}
        
        recommendations = {
            'conservative': {
                'title': 'Консервативный портфель',
                'description': 'Низкий риск, стабильная доходность',
                'criteria': 'Волатильность < 15%, Sharpe > 0.5',
                'etfs': self.enhanced_df[(self.enhanced_df['volatility'] < 15) & 
                                        (self.enhanced_df['sharpe_ratio'] > 0.5)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'balanced': {
                'title': 'Сбалансированный портфель',
                'description': 'Средний риск, умеренная доходность',
                'criteria': 'Волатильность 15-25%, Sharpe > 0.3',
                'etfs': self.enhanced_df[(self.enhanced_df['volatility'] >= 15) & 
                                        (self.enhanced_df['volatility'] <= 25) & 
                                        (self.enhanced_df['sharpe_ratio'] > 0.3)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'aggressive': {
                'title': 'Агрессивный портфель',
                'description': 'Высокий риск, высокая потенциальная доходность',
                'criteria': 'Доходность > 20%',
                'etfs': self.enhanced_df[self.enhanced_df['annual_return'] > 20]
                       .nlargest(5, 'annual_return')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            }
        }
        
        return recommendations

# Создаем глобальный экземпляр дашборда
dashboard = ETFWebDashboard()

@app.route('/')
def index():
    """Главная страница дашборда"""
    return render_template('dashboard.html')

@app.route('/api/summary')
def api_summary():
    """API для получения сводной статистики"""
    return jsonify(dashboard.get_summary_stats())

@app.route('/api/risk-return-plot')
def api_risk_return_plot():
    """API для получения графика риск-доходность"""
    return dashboard.create_risk_return_plot()

@app.route('/api/sector-analysis-plot')
def api_sector_analysis_plot():
    """API для получения секторального анализа"""
    return dashboard.create_sector_analysis_plot()

@app.route('/api/performance-comparison-plot')
def api_performance_comparison_plot():
    """API для получения сравнения доходности"""
    return dashboard.create_performance_comparison_plot()

@app.route('/api/correlation-heatmap')
def api_correlation_heatmap():
    """API для получения корреляционной карты"""
    return dashboard.create_correlation_heatmap()

@app.route('/api/efficient-frontier-plot')
def api_efficient_frontier_plot():
    """API для получения эффективной границы"""
    return dashboard.create_efficient_frontier_plot()

@app.route('/api/recommendations')
def api_recommendations():
    """API для получения инвестиционных рекомендаций"""
    return jsonify(dashboard.get_investment_recommendations())

@app.route('/api/etf-data')
def api_etf_data():
    """API для получения данных ETF"""
    if dashboard.enhanced_df is None:
        return jsonify([])
    
    # Получаем параметр limit из запроса
    limit = request.args.get('limit', 50, type=int)
    limit = min(limit, 100)  # Максимум 100 записей
    
    # Возвращаем топ ETF для таблицы
    top_etfs = dashboard.enhanced_df.nlargest(limit, 'avg_daily_volume')
    
    # Добавляем дополнительные поля если они есть
    columns = ['ticker', 'short_name', 'name', 'category', 'annual_return', 
               'volatility', 'sharpe_ratio', 'avg_daily_volume', 
               'management_company', 'isin']
    
    # Фильтруем только существующие колонки
    available_columns = [col for col in columns if col in top_etfs.columns]
    
    return jsonify(top_etfs[available_columns].to_dict('records'))

if __name__ == '__main__':
    print("🚀 Запуск веб-дашборда ETF аналитики...")
    print("🌐 Откройте браузер и перейдите по адресу: http://localhost:5000")
    print("⏹️  Для остановки нажмите Ctrl+C")
    
    app.run(debug=True, host='0.0.0.0', port=5000)