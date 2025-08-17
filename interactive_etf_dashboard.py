#!/usr/bin/env python3
"""
Интерактивный дашборд для анализа ETF с использованием Plotly
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import json
from datetime import datetime
from pathlib import Path

class InteractiveETFDashboard:
    """Интерактивный дашборд для анализа ETF"""
    
    def __init__(self):
        self.df = None
        self.report = None
    
    def load_data(self):
        """Загружает данные ETF"""
        
        # Ищем последние файлы
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
        
        if not data_files:
            raise FileNotFoundError("Файлы с данными ETF не найдены")
        
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        print(f"📊 Загружаем данные из {latest_data}")
        
        self.df = pd.read_csv(latest_data)
        
        # Загружаем отчет если есть
        report_files = list(Path('.').glob('full_moex_etf_report_*.json'))
        if report_files:
            latest_report = max(report_files, key=lambda x: x.stat().st_mtime)
            with open(latest_report, 'r', encoding='utf-8') as f:
                self.report = json.load(f)
    
    def create_risk_return_scatter(self) -> go.Figure:
        """Создает интерактивный scatter plot риск-доходность"""
        
        valid_data = self.df[(self.df['annual_return'].notna()) & 
                            (self.df['volatility'].notna()) & 
                            (self.df['avg_daily_volume'].notna())]
        
        if len(valid_data) == 0:
            return None
        
        # Создаем scatter plot с размером пузырьков = объем торгов
        fig = px.scatter(
            valid_data, 
            x='volatility', 
            y='annual_return',
            size='avg_daily_volume',
            color='category',
            hover_name='ticker',
            hover_data=['management_company', 'expense_ratio'],
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
        
        fig.update_layout(
            width=1000,
            height=600,
            showlegend=True
        )
        
        return fig
    
    def create_sector_analysis(self) -> go.Figure:
        """Создает анализ по секторам/категориям"""
        
        if self.df is None or len(self.df) == 0:
            return None
        
        # Группируем по категориям
        sector_stats = self.df.groupby('category').agg({
            'annual_return': 'mean',
            'volatility': 'mean',
            'avg_daily_volume': 'sum',
            'ticker': 'count'
        }).round(2)
        
        sector_stats.columns = ['Средняя доходность', 'Средняя волатильность', 
                               'Общий объем', 'Количество ETF']
        
        # Создаем subplot с несколькими графиками
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Доходность по секторам', 'Волатильность по секторам',
                           'Количество ETF', 'Объем торгов'),
            specs=[[{}, {}], [{}, {}]]
        )
        
        categories = sector_stats.index.tolist()
        
        # График 1: Доходность
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['Средняя доходность'], 
                   name='Доходность (%)', marker_color='lightgreen'),
            row=1, col=1
        )
        
        # График 2: Волатильность
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['Средняя волатильность'], 
                   name='Волатильность (%)', marker_color='lightcoral'),
            row=1, col=2
        )
        
        # График 3: Количество ETF
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['Количество ETF'], 
                   name='Количество', marker_color='lightblue'),
            row=2, col=1
        )
        
        # График 4: Объем торгов
        fig.add_trace(
            go.Bar(x=categories, y=sector_stats['Общий объем'], 
                   name='Объем', marker_color='orange'),
            row=2, col=2
        )
        
        fig.update_layout(
            title='🏢 Секторальный анализ ETF',
            height=800,
            showlegend=False
        )
        
        return fig
    
    def create_performance_comparison(self) -> go.Figure:
        """Создает сравнение доходности лидеров и аутсайдеров"""
        
        if self.df is None or len(self.df) == 0:
            return None
        
        # Сортируем по доходности
        df_sorted = self.df.sort_values('annual_return', ascending=True)
        
        # Берем топ и аутсайдеров
        top_performers = df_sorted.tail(10)
        worst_performers = df_sorted.head(10)
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('🏆 ТОП-10 по доходности', '📉 Аутсайдеры'),
            horizontal_spacing=0.1
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
            title='📊 Анализ доходности: лидеры vs аутсайдеры',
            height=600,
            showlegend=False
        )
        
        fig.update_xaxes(title_text="Годовая доходность (%)", row=1, col=1)
        fig.update_xaxes(title_text="Годовая доходность (%)", row=1, col=2)
        
        return fig
    
    def create_expense_ratio_analysis(self) -> go.Figure:
        """Создает анализ комиссий (expense ratio)"""
        
        if self.df is None or len(self.df) == 0:
            return None
        
        # Фильтруем данные с валидными expense_ratio
        valid_data = self.df[self.df['expense_ratio'].notna() & (self.df['expense_ratio'] > 0)]
        
        if len(valid_data) == 0:
            return None
        
        fig = px.scatter(
            valid_data,
            x='expense_ratio',
            y='annual_return',
            size='avg_daily_volume',
            color='category',
            hover_name='ticker',
            title='💰 Анализ комиссий vs доходность',
            labels={
                'expense_ratio': 'Комиссия (%)',
                'annual_return': 'Годовая доходность (%)'
            }
        )
        
        fig.update_layout(
            width=1000,
            height=600
        )
        
        return fig
    
    def create_liquidity_analysis(self) -> go.Figure:
        """Создает анализ ликвидности"""
        
        if self.df is None or len(self.df) == 0:
            return None
        
        # Создаем категории ликвидности
        self.df['liquidity_category'] = pd.cut(
            self.df['avg_daily_volume'], 
            bins=[0, 1000, 10000, 100000, float('inf')],
            labels=['Низкая', 'Средняя', 'Высокая', 'Очень высокая']
        )
        
        # Группируем по ликвидности
        liquidity_stats = self.df.groupby('liquidity_category').agg({
            'annual_return': 'mean',
            'volatility': 'mean',
            'ticker': 'count'
        }).round(2)
        
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=('Доходность по ликвидности', 'Волатильность', 'Количество ETF')
        )
        
        categories = liquidity_stats.index.tolist()
        
        fig.add_trace(
            go.Bar(x=categories, y=liquidity_stats['annual_return'], 
                   marker_color='blue'),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(x=categories, y=liquidity_stats['volatility'], 
                   marker_color='red'),
            row=1, col=2
        )
        
        fig.add_trace(
            go.Bar(x=categories, y=liquidity_stats['ticker'], 
                   marker_color='green'),
            row=1, col=3
        )
        
        fig.update_layout(
            title='💧 Анализ ликвидности ETF',
            height=500,
            showlegend=False
        )
        
        return fig
    
    def create_comprehensive_dashboard(self) -> str:
        """Создает комплексный HTML дашборд"""
        
        print("🎨 Создаем комплексный интерактивный дашборд...")
        
        # Создаем все графики
        risk_return_fig = self.create_risk_return_scatter()
        sector_fig = self.create_sector_analysis()
        performance_fig = self.create_performance_comparison()
        expense_fig = self.create_expense_ratio_analysis()
        liquidity_fig = self.create_liquidity_analysis()
        
        # Создаем HTML дашборд
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"interactive_etf_dashboard_{timestamp}.html"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>📊 Интерактивный дашборд российских ETF</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ text-align: center; color: #2c3e50; margin-bottom: 30px; }}
                .chart-container {{ background: white; margin: 20px 0; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .stats {{ display: flex; justify-content: space-around; background: #e8f4fd; padding: 20px; border-radius: 10px; margin: 20px 0; }}
                .stat-item {{ text-align: center; }}
                .stat-number {{ font-size: 2em; font-weight: bold; color: #2c3e50; }}
                .stat-label {{ color: #7f8c8d; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Интерактивный дашборд российских ETF</h1>
                <h2>Анализ {len(self.df)} фондов на MOEX</h2>
                <p>Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div class="stat-number">{len(self.df)}</div>
                    <div class="stat-label">Всего ETF</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{self.df['annual_return'].mean():.1f}%</div>
                    <div class="stat-label">Средняя доходность</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{self.df['volatility'].mean():.1f}%</div>
                    <div class="stat-label">Средняя волатильность</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{len(self.df['category'].unique())}</div>
                    <div class="stat-label">Категорий активов</div>
                </div>
            </div>
        """
        
        # Добавляем графики
        charts = [
            (risk_return_fig, "risk_return", "📊 Анализ Риск-Доходность"),
            (sector_fig, "sector_analysis", "🏢 Секторальный анализ"),
            (performance_fig, "performance_comparison", "📊 Сравнение доходности"),
            (expense_fig, "expense_analysis", "💰 Анализ комиссий"),
            (liquidity_fig, "liquidity_analysis", "💧 Анализ ликвидности")
        ]
        
        for fig, div_id, title in charts:
            if fig:
                html_content += f"""
                <div class="chart-container">
                    <h3>{title}</h3>
                    <div id="{div_id}"></div>
                    <script>
                        Plotly.newPlot('{div_id}', {fig.to_json()});
                    </script>
                </div>
                """
        
        html_content += """
        <div class="chart-container">
            <h3>📋 Методология анализа</h3>
            <ul>
                <li><strong>Риск-доходность:</strong> Анализ соотношения годовой доходности и волатильности</li>
                <li><strong>Секторальный анализ:</strong> Группировка по категориям активов</li>
                <li><strong>Анализ комиссий:</strong> Влияние expense ratio на доходность</li>
                <li><strong>Ликвидность:</strong> Анализ торговых объемов и их влияния на доходность</li>
                <li><strong>Сравнение доходности:</strong> Выявление лидеров и аутсайдеров рынка</li>
            </ul>
        </div>
        </body>
        </html>
        """
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Интерактивный дашборд создан: {filename}")
        return filename
        
        fig = px.scatter(valid_data, 
                        x='volatility', 
                        y='annual_return',
                        size='avg_daily_volume',
                        color='sharpe_ratio' if 'sharpe_ratio' in valid_data.columns else 'annual_return',
                        hover_name='ticker',
                        hover_data={
                            'short_name': True,
                            'annual_return': ':.2f',
                            'volatility': ':.2f',
                            'avg_daily_volume': ':,.0f',
                            'sharpe_ratio': ':.3f' if 'sharpe_ratio' in valid_data.columns else False
                        },
                        title="Интерактивный анализ риск-доходность",
                        labels={
                            'volatility': 'Волатильность (%)',
                            'annual_return': 'Годовая доходность (%)',
                            'avg_daily_volume': 'Средний объем торгов',
                            'sharpe_ratio': 'Коэффициент Шарпа'
                        },
                        color_continuous_scale='RdYlGn')
        
        fig.update_layout(
            width=800,
            height=600,
            showlegend=True
        )
        
        return fig
    
    def create_correlation_heatmap(self) -> go.Figure:
        """Создает интерактивную корреляционную тепловую карту"""
        
        # Берем топ-20 ETF по ликвидности
        top_etf = self.df.nlargest(20, 'avg_daily_volume')
        
        # Создаем синтетическую корреляционную матрицу
        from correlation_analysis import CorrelationAnalyzer
        analyzer = CorrelationAnalyzer()
        etf_list = top_etf.to_dict('records')
        correlation_matrix = analyzer.generate_synthetic_correlations(etf_list)
        
        fig = px.imshow(correlation_matrix,
                       title="Корреляционная матрица топ-20 ETF",
                       color_continuous_scale='RdBu_r',
                       aspect='auto')
        
        fig.update_layout(
            width=700,
            height=700
        )
        
        return fig
    
    def create_portfolio_composition_sunburst(self) -> go.Figure:
        """Создает sunburst диаграмму состава оптимального портфеля"""
        
        # Классифицируем ETF
        etf_types = {
            'Облигации': [],
            'Акции': [],
            'Золото': [],
            'Валютные': [],
            'Другие': []
        }
        
        for _, row in self.df.iterrows():
            name = str(row.get('short_name', '')).lower()
            ticker = row['ticker']
            annual_return = row.get('annual_return', 0)
            
            if any(word in name for word in ['bond', 'облиг', 'gb', 'cb']):
                etf_types['Облигации'].append((ticker, annual_return))
            elif any(word in name for word in ['gold', 'золот', 'gld']):
                etf_types['Золото'].append((ticker, annual_return))
            elif any(word in name for word in ['usd', 'eur', 'yuan']):
                etf_types['Валютные'].append((ticker, annual_return))
            elif any(word in name for word in ['equity', 'акц', 'mx']):
                etf_types['Акции'].append((ticker, annual_return))
            else:
                etf_types['Другие'].append((ticker, annual_return))
        
        # Подготавливаем данные для sunburst
        ids = []
        labels = []
        parents = []
        values = []
        
        # Добавляем категории
        for category, etfs in etf_types.items():
            if etfs:
                ids.append(category)
                labels.append(f"{category} ({len(etfs)})")
                parents.append("")
                values.append(len(etfs))
                
                # Добавляем топ-3 ETF в каждой категории
                top_etfs = sorted(etfs, key=lambda x: x[1], reverse=True)[:3]
                for ticker, return_val in top_etfs:
                    ids.append(f"{category}_{ticker}")
                    labels.append(f"{ticker} ({return_val:.1f}%)")
                    parents.append(category)
                    values.append(max(1, return_val))
        
        fig = go.Figure(go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            title="Структура рынка ETF по типам активов"
        ))
        
        fig.update_layout(
            width=600,
            height=600
        )
        
        return fig
    
    def create_performance_comparison(self) -> go.Figure:
        """Создает сравнение производительности топ ETF"""
        
        top_etf = self.df.nlargest(10, 'sharpe_ratio' if 'sharpe_ratio' in self.df.columns else 'annual_return')
        
        fig = go.Figure()
        
        # Добавляем бары для доходности
        fig.add_trace(go.Bar(
            name='Годовая доходность',
            x=top_etf['ticker'],
            y=top_etf['annual_return'],
            yaxis='y',
            offsetgroup=1,
            marker_color='lightgreen'
        ))
        
        # Добавляем линию для волатильности
        fig.add_trace(go.Scatter(
            name='Волатильность',
            x=top_etf['ticker'],
            y=top_etf['volatility'],
            yaxis='y2',
            mode='lines+markers',
            line=dict(color='red', width=3),
            marker=dict(size=8)
        ))
        
        # Настраиваем оси
        fig.update_layout(
            title='Сравнение топ-10 ETF: доходность vs волатильность',
            xaxis=dict(title='ETF'),
            yaxis=dict(title='Годовая доходность (%)', side='left'),
            yaxis2=dict(title='Волатильность (%)', side='right', overlaying='y'),
            width=900,
            height=500
        )
        
        return fig
    
    def create_dashboard(self) -> str:
        """Создает полный интерактивный дашборд"""
        
        print("🎨 Создаем интерактивный дашборд...")
        
        # Загружаем данные
        self.load_data()
        
        # Создаем отдельные графики
        risk_return_fig = self.create_risk_return_scatter()
        correlation_fig = self.create_correlation_heatmap()
        sunburst_fig = self.create_portfolio_composition_sunburst()
        performance_fig = self.create_performance_comparison()
        
        # Сохраняем каждый график
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        files_created = []
        
        # Риск-доходность
        risk_return_file = f"interactive_risk_return_{timestamp}.html"
        risk_return_fig.write_html(risk_return_file)
        files_created.append(risk_return_file)
        
        # Корреляции
        correlation_file = f"interactive_correlations_{timestamp}.html"
        correlation_fig.write_html(correlation_file)
        files_created.append(correlation_file)
        
        # Структура рынка
        sunburst_file = f"interactive_market_structure_{timestamp}.html"
        sunburst_fig.write_html(sunburst_file)
        files_created.append(sunburst_file)
        
        # Сравнение производительности
        performance_file = f"interactive_performance_{timestamp}.html"
        performance_fig.write_html(performance_file)
        files_created.append(performance_file)
        
        print("💾 Интерактивные графики сохранены:")
        for file in files_created:
            print(f"   • {file}")
        
        return files_created[0]  # Возвращаем первый файл

def main():
    """Основная функция"""
    
    dashboard = InteractiveETFDashboard()
    result = dashboard.create_dashboard()
    
    if result:
        print("✅ Интерактивный дашборд создан успешно!")
        print(f"🌐 Откройте {result} в браузере для просмотра")

if __name__ == "__main__":
    main()