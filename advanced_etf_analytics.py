#!/usr/bin/env python3
"""
Основной модуль для продвинутой аналитической визуализации ETF
Фокус на инвестиционных инсайтах и принятии решений
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Импортируем наши модули
from risk_metrics import RiskMetricsCalculator, calculate_portfolio_metrics
from correlation_analysis import CorrelationAnalyzer
from portfolio_optimization import PortfolioOptimizer

# Настройка стиля
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (20, 16)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

class AdvancedETFAnalytics:
    """Продвинутая аналитика ETF с фокусом на инвестиционные решения"""
    
    def __init__(self):
        self.risk_calculator = RiskMetricsCalculator()
        self.correlation_analyzer = CorrelationAnalyzer()
        self.portfolio_optimizer = PortfolioOptimizer()
        
    def load_etf_data(self) -> Tuple[pd.DataFrame, Dict]:
        """Загружает последние данные ETF"""
        
        # Ищем последние файлы
        data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
        report_files = list(Path('.').glob('full_moex_etf_report_*.json'))
        
        if not data_files or not report_files:
            raise FileNotFoundError("Файлы с данными ETF не найдены")
        
        # Берем последние файлы
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        latest_report = max(report_files, key=lambda x: x.stat().st_mtime)
        
        print(f"📊 Загружаем данные из {latest_data}")
        
        # Загружаем данные
        df = pd.read_csv(latest_data)
        
        with open(latest_report, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        print(f"✅ Загружено {len(df)} ETF")
        return df, report
    
    def enhance_data_with_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обогащает данные ETF расчетными метриками"""
        
        print("🔄 Рассчитываем дополнительные метрики...")
        
        enhanced_df = df.copy()
        
        # Рассчитываем Sharpe ratio (безрисковая ставка 15%)
        risk_free_rate = 15.0
        enhanced_df['sharpe_ratio'] = (enhanced_df['annual_return'] - risk_free_rate) / enhanced_df['volatility']
        
        # Рассчитываем Sortino ratio (упрощенная версия)
        enhanced_df['sortino_ratio'] = enhanced_df['annual_return'] / (enhanced_df['volatility'] * 0.7)  # Приблизительно
        
        # Рассчитываем альфу и бету (упрощенная версия относительно среднего рынка)
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
        
        print(f"✅ Данные обогащены метриками для {len(enhanced_df)} ETF")
        return enhanced_df
    
    def create_correlation_analysis(self, df: pd.DataFrame) -> np.ndarray:
        """Создает корреляционный анализ топ ETF"""
        
        # Берем топ-20 ETF по объему для анализа корреляций
        top_etfs = df.nlargest(20, 'avg_daily_volume')
        
        # Генерируем синтетическую корреляционную матрицу на основе категорий
        correlation_matrix = self.correlation_analyzer.generate_synthetic_correlations(
            top_etfs.to_dict('records')
        )
        
        return correlation_matrix
    
    def create_efficient_frontier_analysis(self, df: pd.DataFrame) -> Dict:
        """Создает анализ эффективной границы"""
        
        # Берем топ-15 ETF для оптимизации
        top_etfs = df.nlargest(15, 'sharpe_ratio').to_dict('records')
        
        # Подготавливаем данные
        returns, cov_matrix, tickers = self.portfolio_optimizer.prepare_data(top_etfs)
        
        # Строим эффективную границу
        efficient_frontier = self.portfolio_optimizer.build_efficient_frontier(
            returns, cov_matrix, num_portfolios=30
        )
        
        # Находим оптимальные портфели
        optimal_portfolios = self.portfolio_optimizer.find_optimal_portfolios(top_etfs)
        
        return {
            'efficient_frontier': efficient_frontier,
            'optimal_portfolios': optimal_portfolios,
            'tickers': tickers
        }
    
    def generate_investment_recommendations(self) -> Dict:
        """Генерирует инвестиционные рекомендации"""
        
        df, _ = self.load_etf_data()
        enhanced_df = self.enhance_data_with_metrics(df)
        
        recommendations = {
            'conservative_portfolio': {
                'description': 'Консервативный портфель (низкий риск)',
                'criteria': 'Волатильность < 15%, Sharpe > 0.5',
                'etfs': enhanced_df[(enhanced_df['volatility'] < 15) & (enhanced_df['sharpe_ratio'] > 0.5)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'balanced_portfolio': {
                'description': 'Сбалансированный портфель (средний риск)',
                'criteria': 'Волатильность 15-25%, Sharpe > 0.3',
                'etfs': enhanced_df[(enhanced_df['volatility'] >= 15) & (enhanced_df['volatility'] <= 25) & (enhanced_df['sharpe_ratio'] > 0.3)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'aggressive_portfolio': {
                'description': 'Агрессивный портфель (высокий риск)',
                'criteria': 'Доходность > 20%, любая волатильность',
                'etfs': enhanced_df[enhanced_df['annual_return'] > 20]
                       .nlargest(5, 'annual_return')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'diversification_picks': {
                'description': 'Рекомендации для диверсификации',
                'criteria': 'Разные категории активов с хорошими метриками',
                'etfs': enhanced_df.groupby('category').apply(
                    lambda x: x.nlargest(1, 'sharpe_ratio')
                ).reset_index(drop=True)[['ticker', 'category', 'annual_return', 'volatility', 'sharpe_ratio']]
                .to_dict('records')
            }
        }
        
        return recommendations
    
    def create_comprehensive_analysis(self) -> str:
        """Создает комплексный анализ с визуализациями"""
        
        print("🔬 Создаем комплексный анализ...")
        
        # Загружаем и обогащаем данные
        df, report = self.load_etf_data()
        enhanced_df = self.enhance_data_with_metrics(df)
        
        # Создаем анализы
        correlation_matrix = self.create_correlation_analysis(enhanced_df)
        frontier_analysis = self.create_efficient_frontier_analysis(enhanced_df)
        
        # Создаем визуализации
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Создаем большую фигуру с множественными subplot
        fig, axes = plt.subplots(3, 3, figsize=(24, 20))
        fig.suptitle('🔬 Продвинутая аналитика российских ETF', fontsize=20, fontweight='bold')
        
        # График 1: Риск-доходность
        ax1 = axes[0, 0]
        scatter = ax1.scatter(enhanced_df['volatility'], enhanced_df['annual_return'], 
                             s=enhanced_df['avg_daily_volume']/1000, 
                             c=enhanced_df['category'].astype('category').cat.codes, 
                             alpha=0.7, cmap='tab10')
        ax1.set_xlabel('Волатильность (%)')
        ax1.set_ylabel('Годовая доходность (%)')
        ax1.set_title('📊 Риск vs Доходность')
        ax1.axhline(y=15, color='red', linestyle='--', alpha=0.7, label='Безрисковая ставка')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # График 2: Секторальный анализ
        ax2 = axes[0, 1]
        sector_returns = enhanced_df.groupby('category')['annual_return'].mean().sort_values(ascending=True)
        bars = ax2.barh(range(len(sector_returns)), sector_returns.values, color='lightblue')
        ax2.set_yticks(range(len(sector_returns)))
        ax2.set_yticklabels(sector_returns.index, fontsize=9)
        ax2.set_xlabel('Средняя доходность (%)')
        ax2.set_title('🏢 Доходность по секторам')
        ax2.grid(True, alpha=0.3)
        
        # График 3: Корреляционная матрица
        ax3 = axes[0, 2]
        if correlation_matrix is not None and len(correlation_matrix) > 0:
            im = ax3.imshow(correlation_matrix.values, cmap='RdBu', vmin=-1, vmax=1)
            ax3.set_xticks(range(len(correlation_matrix.columns)))
            ax3.set_yticks(range(len(correlation_matrix.index)))
            ax3.set_xticklabels(correlation_matrix.columns, rotation=45, fontsize=8)
            ax3.set_yticklabels(correlation_matrix.index, fontsize=8)
            ax3.set_title('🔗 Корреляционная матрица')
            plt.colorbar(im, ax=ax3, shrink=0.8)
        
        # График 4: Sharpe Ratio распределение
        ax4 = axes[1, 0]
        ax4.hist(enhanced_df['sharpe_ratio'].dropna(), bins=20, alpha=0.7, color='green')
        ax4.axvline(enhanced_df['sharpe_ratio'].mean(), color='red', linestyle='--', 
                   label=f'Среднее: {enhanced_df["sharpe_ratio"].mean():.2f}')
        ax4.set_xlabel('Sharpe Ratio')
        ax4.set_ylabel('Количество ETF')
        ax4.set_title('📈 Распределение Sharpe Ratio')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        # График 5: Топ и худшие исполнители
        ax5 = axes[1, 1]
        top_5 = enhanced_df.nlargest(5, 'annual_return')
        worst_5 = enhanced_df.nsmallest(5, 'annual_return')
        
        y_pos_top = np.arange(len(top_5))
        y_pos_worst = np.arange(len(worst_5)) + len(top_5) + 1
        
        ax5.barh(y_pos_top, top_5['annual_return'], color='green', alpha=0.7, label='Топ-5')
        ax5.barh(y_pos_worst, worst_5['annual_return'], color='red', alpha=0.7, label='Худшие-5')
        
        all_tickers = list(top_5['ticker']) + [''] + list(worst_5['ticker'])
        ax5.set_yticks(list(y_pos_top) + [len(top_5)] + list(y_pos_worst))
        ax5.set_yticklabels(all_tickers, fontsize=9)
        ax5.set_xlabel('Годовая доходность (%)')
        ax5.set_title('🏆 Лидеры и аутсайдеры')
        ax5.legend()
        ax5.grid(True, alpha=0.3)
        
        # График 6: Анализ ликвидности
        ax6 = axes[1, 2]
        liquidity_bins = pd.cut(enhanced_df['avg_daily_volume'], bins=5, labels=['Очень низкая', 'Низкая', 'Средняя', 'Высокая', 'Очень высокая'])
        liquidity_returns = enhanced_df.groupby(liquidity_bins)['annual_return'].mean()
        
        bars = ax6.bar(range(len(liquidity_returns)), liquidity_returns.values, color='orange', alpha=0.7)
        ax6.set_xticks(range(len(liquidity_returns)))
        ax6.set_xticklabels(liquidity_returns.index, rotation=45, fontsize=9)
        ax6.set_ylabel('Средняя доходность (%)')
        ax6.set_title('💧 Доходность vs Ликвидность')
        ax6.grid(True, alpha=0.3)
        
        # График 7: Эффективная граница
        ax7 = axes[2, 0]
        if frontier_analysis and 'efficient_frontier' in frontier_analysis:
            ef = frontier_analysis['efficient_frontier']
            ax7.plot([v * 100 for v in ef['volatilities']], [r * 100 for r in ef['returns']], 
                    'b-', linewidth=2, label='Эффективная граница')
            
            # Добавляем индивидуальные ETF
            top_15 = enhanced_df.nlargest(15, 'sharpe_ratio')
            ax7.scatter(top_15['volatility'], top_15['annual_return'], 
                       alpha=0.6, color='red', s=50, label='Индивидуальные ETF')
            
            ax7.set_xlabel('Волатильность (%)')
            ax7.set_ylabel('Ожидаемая доходность (%)')
            ax7.set_title('📈 Эффективная граница')
            ax7.legend()
            ax7.grid(True, alpha=0.3)
        
        # График 8: Анализ комиссий
        ax8 = axes[2, 1]
        valid_expense = enhanced_df[enhanced_df['expense_ratio'].notna() & (enhanced_df['expense_ratio'] > 0)]
        if len(valid_expense) > 0:
            ax8.scatter(valid_expense['expense_ratio'], valid_expense['annual_return'], 
                       alpha=0.7, color='purple')
            ax8.set_xlabel('Комиссия (%)')
            ax8.set_ylabel('Годовая доходность (%)')
            ax8.set_title('💰 Комиссии vs Доходность')
            ax8.grid(True, alpha=0.3)
        
        # График 9: Сводная статистика
        ax9 = axes[2, 2]
        ax9.axis('off')
        
        stats_text = f"""
        📊 СВОДНАЯ СТАТИСТИКА
        
        Всего ETF: {len(enhanced_df)}
        Средняя доходность: {enhanced_df['annual_return'].mean():.1f}%
        Средняя волатильность: {enhanced_df['volatility'].mean():.1f}%
        Средний Sharpe: {enhanced_df['sharpe_ratio'].mean():.2f}
        
        Лучший ETF: {enhanced_df.loc[enhanced_df['annual_return'].idxmax(), 'ticker']}
        ({enhanced_df['annual_return'].max():.1f}%)
        
        Самый стабильный: {enhanced_df.loc[enhanced_df['volatility'].idxmin(), 'ticker']}
        ({enhanced_df['volatility'].min():.1f}% волатильность)
        
        Лучший Sharpe: {enhanced_df.loc[enhanced_df['sharpe_ratio'].idxmax(), 'ticker']}
        ({enhanced_df['sharpe_ratio'].max():.2f})
        """
        
        ax9.text(0.1, 0.9, stats_text, transform=ax9.transAxes, fontsize=11,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        plt.tight_layout()
        
        # Сохраняем изображение
        image_filename = f"advanced_etf_analytics_{timestamp}.png"
        plt.savefig(image_filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Сохраняем обогащенные данные
        data_filename = f"enhanced_etf_data_{timestamp}.csv"
        enhanced_df.to_csv(data_filename, index=False, encoding='utf-8')
        
        print(f"✅ Продвинутая аналитика создана:")
        print(f"   • Изображение: {image_filename}")
        print(f"   • Данные: {data_filename}")
        
        return image_filename
        
        print(f"📊 Загружаем данные из {latest_data}")
        
        # Загружаем данные
        df = pd.read_csv(latest_data)
        
        with open(latest_report, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        return df, report
    
    def calculate_enhanced_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Рассчитывает расширенные метрики для всех ETF"""
        
        print("🔢 Рассчитываем расширенные риск-метрики...")
        
        enhanced_data = []
        
        for _, row in df.iterrows():
            etf_data = row.to_dict()
            
            # Рассчитываем все риск-метрики
            risk_metrics = self.risk_calculator.calculate_all_metrics(etf_data)
            
            # Объединяем данные
            enhanced_etf = {**etf_data, **risk_metrics}
            enhanced_data.append(enhanced_etf)
        
        return pd.DataFrame(enhanced_data)
    
    def create_investment_focused_visualization(self, df: pd.DataFrame) -> plt.Figure:
        """Создает визуализацию с фокусом на инвестиционные решения"""
        
        print("🎨 Создаем продвинутую аналитическую визуализацию...")
        
        # Создаем фигуру
        fig = plt.figure(figsize=(24, 20))
        fig.suptitle('📈 ПРОДВИНУТАЯ АНАЛИТИКА РОССИЙСКИХ ETF\nИнвестиционные инсайты и возможности', 
                     fontsize=18, fontweight='bold', y=0.98)
        
        # 1. Риск-доходность с размером пузырьков = ликвидность
        ax1 = plt.subplot(3, 4, 1)
        valid_data = df[(df['annual_return'].notna()) & (df['volatility'].notna()) & (df['avg_daily_volume'].notna())]
        
        scatter = ax1.scatter(valid_data['volatility'], valid_data['annual_return'], 
                             s=valid_data['avg_daily_volume']/1e6*10,  # Размер = объем торгов
                             c=valid_data['sharpe_ratio'], cmap='RdYlGn', 
                             alpha=0.7, edgecolors='black', linewidth=0.5)
        
        # Добавляем подписи для лучших ETF
        top_sharpe = valid_data.nlargest(5, 'sharpe_ratio')
        for _, etf in top_sharpe.iterrows():
            ax1.annotate(etf['ticker'], 
                        (etf['volatility'], etf['annual_return']),
                        xytext=(5, 5), textcoords='offset points', 
                        fontsize=9, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
        
        ax1.set_xlabel('Волатильность (%)')
        ax1.set_ylabel('Годовая доходность (%)')
        ax1.set_title('Риск vs Доходность\n(размер = ликвидность, цвет = Sharpe)')
        ax1.grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=ax1, label='Sharpe Ratio')
        
        # 2. Корреляционная тепловая карта (топ-15 ETF)
        ax2 = plt.subplot(3, 4, 2)
        top_etf = valid_data.nlargest(15, 'avg_daily_volume')
        etf_list = top_etf.to_dict('records')
        
        correlation_matrix = self.correlation_analyzer.generate_synthetic_correlations(etf_list)
        
        sns.heatmap(correlation_matrix, annot=True, cmap='RdBu_r', center=0,
                   square=True, ax=ax2, cbar_kws={'label': 'Корреляция'})
        ax2.set_title('Корреляционная матрица\n(топ-15 по ликвидности)')
        ax2.tick_params(axis='x', rotation=45)
        ax2.tick_params(axis='y', rotation=0)
        
        # 3. Эффективная граница портфеля
        ax3 = plt.subplot(3, 4, 3)
        
        # Берем топ-10 ETF для построения границы
        top_10_etf = valid_data.nlargest(10, 'sharpe_ratio').to_dict('records')
        
        if len(top_10_etf) >= 3:
            returns, cov_matrix, tickers = self.portfolio_optimizer.prepare_data(top_10_etf)
            efficient_frontier = self.portfolio_optimizer.build_efficient_frontier(returns, cov_matrix, 30)
            
            if efficient_frontier['portfolios']:
                ax3.plot(efficient_frontier['volatilities'], efficient_frontier['returns'], 
                        'b-', linewidth=2, label='Эффективная граница')
                    
    
            # Находим оптимальные портфели
            optimal_portfolios = self.portfolio_optimizer.find_optimal_portfolios(top_10_etf)
            
            # Отмечаем оптимальные портфели
            for name, portfolio in optimal_portfolios.items():
                if name in ['maximum_sharpe', 'minimum_variance']:
                    ax3.scatter(portfolio['volatility'], portfolio['return'], 
                              s=100, label=f"{name.replace('_', ' ').title()}")
            
            ax3.set_xlabel('Волатильность (%)')
            ax3.set_ylabel('Доходность (%)')
            ax3.set_title('Эффективная граница\n(топ-10 ETF по Sharpe)')
            ax3.legend()
            ax3.grid(True, alpha=0.3)
        
        # 4. Анализ риск-скорректированной доходности
        ax4 = plt.subplot(3, 4, 4)
        
        # Сортируем по Sharpe ratio
        sharpe_data = valid_data.nlargest(15, 'sharpe_ratio')
        
        bars = ax4.barh(range(len(sharpe_data)), sharpe_data['sharpe_ratio'],
                       color=plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(sharpe_data))))
        
        ax4.set_yticks(range(len(sharpe_data)))
        ax4.set_yticklabels(sharpe_data['ticker'])
        ax4.set_xlabel('Коэффициент Шарпа')
        ax4.set_title('Топ-15 ETF по Sharpe Ratio\n(риск-скорректированная доходность)')
        ax4.grid(axis='x', alpha=0.3)
        
        # Добавляем значения на бары
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax4.text(width + 0.01, bar.get_y() + bar.get_height()/2, 
                    f'{width:.2f}', ha='left', va='center', fontweight='bold')
        
        # 5. Секторальный анализ (по типам ETF)
        ax5 = plt.subplot(3, 4, 5)
        
        # Классифицируем ETF по типам
        etf_types = self._classify_etf_types(valid_data)
        type_performance = {}
        
        for etf_type, etfs in etf_types.items():
            if etfs:
                avg_return = np.mean([etf['annual_return'] for etf in etfs])
                avg_vol = np.mean([etf['volatility'] for etf in etfs])
                count = len(etfs)
                type_performance[etf_type] = {
                    'return': avg_return,
                    'volatility': avg_vol,
                    'count': count
                }
        
        # Создаем scatter plot по типам
        for etf_type, perf in type_performance.items():
            ax5.scatter(perf['volatility'], perf['return'], 
                       s=perf['count']*20, label=f"{etf_type} ({perf['count']})",
                       alpha=0.7)
        
        ax5.set_xlabel('Средняя волатильность (%)')
        ax5.set_ylabel('Средняя доходность (%)')
        ax5.set_title('Анализ по типам активов\n(размер = количество ETF)')
        ax5.legend()
        ax5.grid(True, alpha=0.3)
        
        # 6. Анализ максимальных просадок
        ax6 = plt.subplot(3, 4, 6)
        
        drawdown_data = valid_data.nsmallest(15, 'max_drawdown')  # Наименьшие просадки (лучшие)
        
        bars = ax6.barh(range(len(drawdown_data)), drawdown_data['max_drawdown'],
                       color=plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(drawdown_data))))
        
        ax6.set_yticks(range(len(drawdown_data)))
        ax6.set_yticklabels(drawdown_data['ticker'])
        ax6.set_xlabel('Максимальная просадка (%)')
        ax6.set_title('Топ-15 ETF по устойчивости\n(наименьшие просадки)')
        ax6.grid(axis='x', alpha=0.3)
        
        # 7. Value at Risk (VaR) анализ
        ax7 = plt.subplot(3, 4, 7)
        
        var_data = valid_data.nlargest(15, 'var_95')  # Наибольшие VaR (наиболее рискованные)
        
        ax7.scatter(var_data['var_95'], var_data['annual_return'], 
                   s=60, c=var_data['volatility'], cmap='Reds', alpha=0.7)
        
        ax7.set_xlabel('VaR 95% (дневной риск, %)')
        ax7.set_ylabel('Годовая доходность (%)')
        ax7.set_title('Анализ Value at Risk\n(цвет = волатильность)')
        ax7.grid(True, alpha=0.3)
        
        # 8. Альфа vs Бета анализ
        ax8 = plt.subplot(3, 4, 8)
        
        ax8.scatter(valid_data['beta'], valid_data['alpha'], 
                   s=60, c=valid_data['annual_return'], cmap='RdYlGn', alpha=0.7)
        
        # Добавляем квадранты
        ax8.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax8.axvline(x=1, color='black', linestyle='--', alpha=0.5)
        
        # Подписываем квадранты
        ax8.text(0.5, 2, 'Низкий риск\nВысокая альфа', ha='center', va='center',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
        ax8.text(1.5, 2, 'Высокий риск\nВысокая альфа', ha='center', va='center',
                bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))
        ax8.text(0.5, -2, 'Низкий риск\nНизкая альфа', ha='center', va='center',
                bbox=dict(boxstyle='round', facecolor='orange', alpha=0.7))
        ax8.text(1.5, -2, 'Высокий риск\nНизкая альфа', ha='center', va='center',
                bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.7))
        
        ax8.set_xlabel('Бета (систематический риск)')
        ax8.set_ylabel('Альфа (избыточная доходность, %)')
        ax8.set_title('Альфа vs Бета анализ\n(цвет = доходность)')
        ax8.grid(True, alpha=0.3)
        
        # 9. Диверсификационные возможности
        ax9 = plt.subplot(3, 4, 9)
        
        # Находим диверсификационные возможности
        top_20_etf = valid_data.nlargest(20, 'avg_daily_volume').to_dict('records')
        correlation_matrix = self.correlation_analyzer.generate_synthetic_correlations(top_20_etf)
        diversification_opps = self.correlation_analyzer.find_diversification_opportunities(
            correlation_matrix, top_20_etf, threshold=0.3)
        
        if diversification_opps:
            # Показываем топ-10 возможностей
            top_opps = diversification_opps[:10]
            
            benefits = [opp['diversification_benefit'] for opp in top_opps]
            correlations = [abs(opp['correlation']) for opp in top_opps]
            labels = [f"{opp['etf1']}-{opp['etf2']}" for opp in top_opps]
            
            bars = ax9.barh(range(len(top_opps)), benefits,
                           color=plt.cm.Greens(np.linspace(0.4, 0.9, len(top_opps))))
            
            ax9.set_yticks(range(len(top_opps)))
            ax9.set_yticklabels(labels, fontsize=9)
            ax9.set_xlabel('Выгода от диверсификации (%)')
            ax9.set_title('Топ-10 диверсификационных\nвозможностей')
            ax9.grid(axis='x', alpha=0.3)
        
        # 10. Инвестиционные рекомендации
        ax10 = plt.subplot(3, 4, 10)
        ax10.axis('off')
        
        # Создаем инвестиционные рекомендации
        recommendations = self._generate_investment_recommendations(valid_data)
        
        rec_text = "🎯 ИНВЕСТИЦИОННЫЕ РЕКОМЕНДАЦИИ:\n\n"
        
        for category, recs in recommendations.items():
            rec_text += f"📊 {category.upper()}:\n"
            for rec in recs[:3]:  # Топ-3 в каждой категории
                rec_text += f"• {rec['ticker']}: {rec['reason']}\n"
            rec_text += "\n"
        
        ax10.text(0.05, 0.95, rec_text, transform=ax10.transAxes, fontsize=10,
                 verticalalignment='top', 
                 bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        # 11. Оптимальные портфели
        ax11 = plt.subplot(3, 4, 11)
        
        # Показываем состав оптимальных портфелей
        if len(top_10_etf) >= 3:
            optimal_portfolios = self.portfolio_optimizer.find_optimal_portfolios(top_10_etf)
            
            # Берем портфель максимального Sharpe
            max_sharpe_portfolio = optimal_portfolios['maximum_sharpe']
            weights = list(max_sharpe_portfolio['weights'].values())
            tickers = list(max_sharpe_portfolio['weights'].keys())
            
            # Показываем только значимые веса (>1%)
            significant_weights = [(t, w) for t, w in zip(tickers, weights) if w > 0.01]
            significant_weights.sort(key=lambda x: x[1], reverse=True)
            
            if significant_weights:
                sig_tickers, sig_weights = zip(*significant_weights)
                
                wedges, texts, autotexts = ax11.pie(sig_weights, labels=sig_tickers, 
                                                   autopct='%1.1f%%', startangle=90)
                ax11.set_title(f'Оптимальный портфель\n(Sharpe: {max_sharpe_portfolio["sharpe_ratio"]:.2f})')
        
        # 12. Ключевые инсайты
        ax12 = plt.subplot(3, 4, 12)
        ax12.axis('off')
        
        # Генерируем ключевые инсайты
        insights = self._generate_key_insights(valid_data, correlation_matrix)
        
        insights_text = "💡 КЛЮЧЕВЫЕ ИНСАЙТЫ:\n\n"
        for insight in insights:
            insights_text += f"• {insight}\n\n"
        
        ax12.text(0.05, 0.95, insights_text, transform=ax12.transAxes, fontsize=10,
                 verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
        
        plt.tight_layout()
        return fig
    
    def _classify_etf_types(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Классифицирует ETF по типам активов"""
        
        types = {
            'Облигации': [],
            'Акции': [],
            'Золото/Сырье': [],
            'Валютные': [],
            'Смешанные': []
        }
        
        for _, row in df.iterrows():
            etf = row.to_dict()
            name = str(etf.get('short_name', '')).lower()
            
            if any(word in name for word in ['bond', 'облиг', 'gb', 'cb']):
                types['Облигации'].append(etf)
            elif any(word in name for word in ['gold', 'золот', 'gld']):
                types['Золото/Сырье'].append(etf)
            elif any(word in name for word in ['usd', 'eur', 'yuan', 'валют']):
                types['Валютные'].append(etf)
            elif any(word in name for word in ['equity', 'акц', 'mx', 'index']):
                types['Акции'].append(etf)
            else:
                types['Смешанные'].append(etf)
        
        return types
    
    def _generate_investment_recommendations(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Генерирует инвестиционные рекомендации"""
        
        recommendations = {
            'Консервативные': [],
            'Сбалансированные': [],
            'Агрессивные': [],
            'Высокая ликвидность': []
        }
        
        # Консервативные (низкий риск, стабильная доходность)
        conservative = df[(df['volatility'] < df['volatility'].quantile(0.33)) & 
                         (df['annual_return'] > 0)]
        for _, etf in conservative.nlargest(5, 'sharpe_ratio').iterrows():
            recommendations['Консервативные'].append({
                'ticker': etf['ticker'],
                'reason': f"Низкий риск ({etf['volatility']:.1f}%), доходность {etf['annual_return']:.1f}%"
            })
        
        # Сбалансированные (средний риск, хорошая доходность)
        balanced = df[(df['volatility'] >= df['volatility'].quantile(0.33)) & 
                     (df['volatility'] <= df['volatility'].quantile(0.67)) &
                     (df['sharpe_ratio'] > df['sharpe_ratio'].median())]
        for _, etf in balanced.nlargest(5, 'sharpe_ratio').iterrows():
            recommendations['Сбалансированные'].append({
                'ticker': etf['ticker'],
                'reason': f"Sharpe {etf['sharpe_ratio']:.2f}, доходность {etf['annual_return']:.1f}%"
            })
        
        # Агрессивные (высокая доходность)
        aggressive = df.nlargest(10, 'annual_return')
        for _, etf in aggressive.iterrows():
            recommendations['Агрессивные'].append({
                'ticker': etf['ticker'],
                'reason': f"Высокая доходность {etf['annual_return']:.1f}% (риск {etf['volatility']:.1f}%)"
            })
        
        # Высокая ликвидность
        liquid = df.nlargest(10, 'avg_daily_volume')
        for _, etf in liquid.iterrows():
            recommendations['Высокая ликвидность'].append({
                'ticker': etf['ticker'],
                'reason': f"Объем {etf['avg_daily_volume']/1e6:.0f}М руб/день"
            })
        
        return recommendations
    
    def _generate_key_insights(self, df: pd.DataFrame, correlation_matrix: pd.DataFrame) -> List[str]:
        """Генерирует ключевые инсайты для инвесторов"""
        
        insights = []
        
        # Лучший ETF по Sharpe
        best_sharpe = df.loc[df['sharpe_ratio'].idxmax()]
        insights.append(f"Лучший риск-скорректированный ETF: {best_sharpe['ticker']} "
                       f"(Sharpe: {best_sharpe['sharpe_ratio']:.2f})")
        
        # Средняя корреляция рынка
        correlations = []
        for i in range(len(correlation_matrix.index)):
            for j in range(i+1, len(correlation_matrix.columns)):
                correlations.append(correlation_matrix.iloc[i, j])
        
        avg_correlation = np.mean(correlations)
        insights.append(f"Средняя корреляция рынка: {avg_correlation:.2f} "
                       f"({'высокая' if avg_correlation > 0.6 else 'умеренная' if avg_correlation > 0.3 else 'низкая'} связанность)")
        
        # Диверсификационные возможности
        low_corr_count = sum(1 for c in correlations if abs(c) < 0.3)
        insights.append(f"Диверсификационных возможностей: {low_corr_count} пар с корреляцией <0.3")
        
        # Анализ по типам активов
        etf_types = self._classify_etf_types(df)
        largest_category = max(etf_types.items(), key=lambda x: len(x[1]))
        insights.append(f"Доминирующий тип активов: {largest_category[0]} ({len(largest_category[1])} ETF)")
        
        # Волатильность рынка
        avg_vol = df['volatility'].mean()
        insights.append(f"Средняя волатильность рынка: {avg_vol:.1f}% "
                       f"({'высокая' if avg_vol > 20 else 'умеренная' if avg_vol > 15 else 'низкая'})")
        
        return insights
    
    def run_analysis(self) -> str:
        """Запускает полный анализ и создает визуализацию"""
        
        try:
            # Загружаем данные
            df, report = self.load_etf_data()
            print(f"📊 Загружено {len(df)} ETF для анализа")
            
            # Рассчитываем расширенные метрики
            enhanced_df = self.calculate_enhanced_metrics(df)
            
            # Создаем визуализацию
            fig = self.create_investment_focused_visualization(enhanced_df)
            
            # Сохраняем
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"advanced_etf_analytics_{timestamp}.png"
            
            fig.savefig(filename, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"💾 Продвинутая аналитика сохранена: {filename}")
            
            # Сохраняем расширенные данные
            enhanced_data_file = f"enhanced_etf_data_{timestamp}.csv"
            enhanced_df.to_csv(enhanced_data_file, index=False)
            print(f"💾 Расширенные данные сохранены: {enhanced_data_file}")
            
            plt.show()
            
            return filename
            
        except Exception as e:
            print(f"❌ Ошибка при создании аналитики: {e}")
            import traceback
            traceback.print_exc()
            return None

def main():
    """Основная функция"""
    
    print("🚀 Запускаем продвинутую аналитику ETF...")
    
    analytics = AdvancedETFAnalytics()
    result = analytics.run_analysis()
    
    if result:
        print("✅ Продвинутая аналитика создана успешно!")
        print("🎯 Фокус на инвестиционных инсайтах и возможностях")
    else:
        print("❌ Не удалось создать аналитику")

if __name__ == "__main__":
    main()