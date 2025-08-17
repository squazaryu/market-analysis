"""
Создание визуализаций для анализа российских БПИФов
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from russian_etf_dataset import RussianETFDataset

# Настройка matplotlib для корректного отображения русского текста
plt.rcParams['font.family'] = ['Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def create_market_analysis_charts():
    """
    Создание комплекса графиков для анализа рынка БПИФов
    """
    
    # Загрузка данных
    dataset = RussianETFDataset()
    df = dataset.etf_data
    
    # Создание фигуры с подграфиками
    fig = plt.figure(figsize=(20, 16))
    
    # 1. Доли рынка управляющих компаний (Pie chart)
    ax1 = plt.subplot(3, 3, 1)
    company_assets = df.groupby('management_company')['assets_under_management_mln_rub'].sum()
    colors = plt.cm.Set3(np.linspace(0, 1, len(company_assets)))
    
    wedges, texts, autotexts = ax1.pie(company_assets.values, 
                                      labels=[name.split(' - ')[0] if ' - ' in name else name.split(' ')[0] 
                                             for name in company_assets.index],
                                      autopct='%1.1f%%',
                                      colors=colors,
                                      startangle=90)
    ax1.set_title('Доли рынка по активам под управлением', fontsize=12, fontweight='bold')
    
    # 2. Сравнение доходности по управляющим компаниям (Bar chart)
    ax2 = plt.subplot(3, 3, 2)
    company_performance = df.groupby('management_company')['performance_1y'].mean().sort_values(ascending=True)
    bars = ax2.barh(range(len(company_performance)), company_performance.values, 
                    color=plt.cm.viridis(np.linspace(0, 1, len(company_performance))))
    ax2.set_yticks(range(len(company_performance)))
    ax2.set_yticklabels([name.split(' ')[0] for name in company_performance.index], fontsize=10)
    ax2.set_xlabel('Доходность за год (%)')
    ax2.set_title('Средняя доходность по УК', fontsize=12, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # 3. Распределение по категориям фондов (Bar chart)
    ax3 = plt.subplot(3, 3, 3)
    category_counts = df['category'].value_counts()
    bars = ax3.bar(category_counts.index, category_counts.values, 
                   color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'])
    ax3.set_title('Количество фондов по категориям', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Количество фондов')
    plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')
    
    # 4. Соотношение риск-доходность (Scatter plot)
    ax4 = plt.subplot(3, 3, 4)
    categories = df['category'].unique()
    colors_map = {category: plt.cm.tab10(i) for i, category in enumerate(categories)}
    
    for category in categories:
        category_data = df[df['category'] == category]
        ax4.scatter(category_data['volatility_1y'], category_data['performance_1y'], 
                   c=[colors_map[category]], label=category, s=100, alpha=0.7)
    
    ax4.set_xlabel('Волатильность (%)')
    ax4.set_ylabel('Доходность за год (%)')
    ax4.set_title('Соотношение риск-доходность', fontsize=12, fontweight='bold')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # 5. Комиссии управляющих компаний (Box plot)
    ax5 = plt.subplot(3, 3, 5)
    company_short_names = [name.split(' ')[0] for name in df['management_company']]
    df_plot = df.copy()
    df_plot['company_short'] = company_short_names
    
    sns.boxplot(data=df_plot, x='company_short', y='expense_ratio', ax=ax5)
    ax5.set_title('Распределение комиссий по УК', fontsize=12, fontweight='bold')
    ax5.set_xlabel('Управляющая компания')
    ax5.set_ylabel('Комиссия (%)')
    plt.setp(ax5.get_xticklabels(), rotation=45, ha='right')
    
    # 6. Динамика доходности по периодам (Line plot)
    ax6 = plt.subplot(3, 3, 6)
    periods = ['1m', '3m', '6m', '1y']
    period_columns = ['performance_1m', 'performance_3m', 'performance_6m', 'performance_1y']
    
    # Берем топ-5 фондов по годовой доходности
    top_funds = df.nlargest(5, 'performance_1y')
    
    for idx, fund in top_funds.iterrows():
        values = [fund[col] for col in period_columns]
        ax6.plot(periods, values, marker='o', label=fund['ticker'], linewidth=2)
    
    ax6.set_title('Динамика доходности топ-фондов', fontsize=12, fontweight='bold')
    ax6.set_xlabel('Период')
    ax6.set_ylabel('Доходность (%)')
    ax6.legend()
    ax6.grid(True, alpha=0.3)
    
    # 7. Активы под управлением по категориям (Stacked bar)
    ax7 = plt.subplot(3, 3, 7)
    category_assets = df.groupby(['management_company', 'category'])['assets_under_management_mln_rub'].sum().unstack(fill_value=0)
    category_assets.plot(kind='bar', stacked=True, ax=ax7, colormap='tab10')
    ax7.set_title('Активы по УК и категориям', fontsize=12, fontweight='bold')
    ax7.set_xlabel('Управляющая компания')
    ax7.set_ylabel('Активы (млн руб)')
    ax7.legend(title='Категория', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.setp(ax7.get_xticklabels(), rotation=45, ha='right')
    
    # 8. Коэффициент Шарпа по фондам (Horizontal bar)
    ax8 = plt.subplot(3, 3, 8)
    sharpe_data = df.sort_values('sharpe_ratio', ascending=True)
    bars = ax8.barh(range(len(sharpe_data)), sharpe_data['sharpe_ratio'], 
                    color=plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(sharpe_data))))
    ax8.set_yticks(range(len(sharpe_data)))
    ax8.set_yticklabels(sharpe_data['ticker'])
    ax8.set_xlabel('Коэффициент Шарпа')
    ax8.set_title('Риск-корректированная доходность', fontsize=12, fontweight='bold')
    ax8.grid(axis='x', alpha=0.3)
    
    # 9. Ликвидность фондов (Pie chart)
    ax9 = plt.subplot(3, 3, 9)
    liquidity_counts = df['liquidity_rating'].value_counts()
    colors_liquidity = ['#2ecc71', '#f39c12', '#e74c3c', '#9b59b6']
    ax9.pie(liquidity_counts.values, labels=liquidity_counts.index, 
            autopct='%1.1f%%', colors=colors_liquidity[:len(liquidity_counts)])
    ax9.set_title('Распределение по ликвидности', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.subplots_adjust(hspace=0.3, wspace=0.3)
    
    # Сохранение графика
    plt.savefig('/Users/tumowuh/Desktop/market analysis/etf_market_analysis.png', 
                dpi=300, bbox_inches='tight', facecolor='white')
    print("Графики сохранены в файл: etf_market_analysis.png")
    
    plt.show()

def create_performance_heatmap():
    """
    Создание тепловой карты доходности
    """
    dataset = RussianETFDataset()
    df = dataset.etf_data
    
    # Подготовка данных для тепловой карты
    performance_data = df.set_index('ticker')[['performance_1m', 'performance_3m', 
                                              'performance_6m', 'performance_1y', 'performance_ytd']]
    performance_data.columns = ['1 месяц', '3 месяца', '6 месяцев', '1 год', 'С начала года']
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(performance_data, annot=True, cmap='RdYlGn', center=0, 
                fmt='.1f', cbar_kws={'label': 'Доходность (%)'})
    plt.title('Тепловая карта доходности БПИФов по периодам', fontsize=16, fontweight='bold')
    plt.xlabel('Период')
    plt.ylabel('Тикер фонда')
    
    plt.tight_layout()
    plt.savefig('/Users/tumowuh/Desktop/market analysis/performance_heatmap.png', 
                dpi=300, bbox_inches='tight', facecolor='white')
    print("Тепловая карта сохранена в файл: performance_heatmap.png")
    
    plt.show()

def create_summary_statistics():
    """
    Создание сводной статистики
    """
    dataset = RussianETFDataset()
    df = dataset.etf_data
    
    print("=== СВОДНАЯ СТАТИСТИКА РОССИЙСКИХ БПИФ ===\n")
    
    print("1. ОБЩИЕ ПОКАЗАТЕЛИ РЫНКА:")
    print(f"Общее количество фондов: {len(df)}")
    print(f"Общие активы под управлением: {df['assets_under_management_mln_rub'].sum():,.0f} млн руб")
    print(f"Средняя комиссия: {df['expense_ratio'].mean():.2f}%")
    print(f"Средняя доходность за год: {df['performance_1y'].mean():.1f}%")
    print(f"Средняя волатильность: {df['volatility_1y'].mean():.1f}%")
    print()
    
    print("2. ЛИДЕРЫ ПО ПОКАЗАТЕЛЯМ:")
    print("Максимальная доходность за год:")
    max_perf = df.loc[df['performance_1y'].idxmax()]
    print(f"  {max_perf['ticker']} ({max_perf['name']}): {max_perf['performance_1y']:.1f}%")
    
    print("Минимальная волатильность:")
    min_vol = df.loc[df['volatility_1y'].idxmin()]
    print(f"  {min_vol['ticker']} ({min_vol['name']}): {min_vol['volatility_1y']:.1f}%")
    
    print("Максимальный коэффициент Шарпа:")
    max_sharpe = df.loc[df['sharpe_ratio'].idxmax()]
    print(f"  {max_sharpe['ticker']} ({max_sharpe['name']}): {max_sharpe['sharpe_ratio']:.2f}")
    print()

if __name__ == "__main__":
    print("Создание визуализаций для анализа российских БПИФов...")
    
    # 1. Основные графики
    create_market_analysis_charts()
    
    # 2. Тепловая карта
    create_performance_heatmap()
    
    # 3. Сводная статистика
    create_summary_statistics()
    
    print("\nВсе визуализации созданы успешно!")