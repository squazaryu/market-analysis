"""
Запуск полного анализа российских БПИФов с fallback системой
"""

import json
import pandas as pd
from datetime import datetime
from etf_data_collector import ETFDataCollectorWithFallback
from logger_config import logger


def main():
    """Основная функция для запуска анализа БПИФов"""
    logger.info("=== ЗАПУСК ПОЛНОГО АНАЛИЗА РОССИЙСКИХ БПИФ ===")
    
    # Создаем коллектор
    collector = ETFDataCollectorWithFallback()
    
    # Проверяем статус провайдеров
    logger.info("Статус провайдеров данных:")
    provider_status = collector.get_provider_status()
    
    active_providers = 0
    for provider in provider_status['providers']:
        status_emoji = "✅" if provider['status'] == 'active' else ("⚠️" if provider['status'] == 'degraded' else "❌")
        logger.info(f"  {status_emoji} {provider['name']}: {provider['status']}")
        if provider['status'] in ['active', 'degraded']:
            active_providers += 1
    
    logger.info(f"Активных провайдеров: {active_providers}/{provider_status['total_providers']}")
    
    if active_providers == 0:
        logger.warning("Все провайдеры недоступны! Будут использованы только кэшированные данные и метаданные.")
    
    # Собираем данные по всем БПИФам
    logger.info("\nСбор данных по всем российским БПИФам...")
    etf_df = collector.collect_all_etf_data()
    
    # Сохраняем сырые данные
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_data_file = f'etf_raw_data_{timestamp}.csv'
    etf_df.to_csv(raw_data_file, index=False, encoding='utf-8')
    logger.info(f"Сырые данные сохранены в: {raw_data_file}")
    
    # Создаем комплексный отчет
    logger.info("\nСоздание комплексного отчета...")
    comprehensive_report = collector.create_comprehensive_report()
    
    # Сохраняем отчет
    report_file = f'etf_comprehensive_report_{timestamp}.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(comprehensive_report, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Комплексный отчет сохранен в: {report_file}")
    
    # Выводим ключевые результаты
    print_key_results(comprehensive_report, etf_df)
    
    # Создаем визуализации
    create_visualizations(etf_df, timestamp)
    
    logger.info(f"\n=== АНАЛИЗ ЗАВЕРШЕН ===")
    logger.info(f"Файлы созданы:")
    logger.info(f"  📊 Данные: {raw_data_file}")
    logger.info(f"  📋 Отчет: {report_file}")
    logger.info(f"  📈 Графики: etf_analysis_charts_{timestamp}.png")


def print_key_results(report, df):
    """Вывод ключевых результатов анализа"""
    print("\n" + "="*60)
    print("🏆 КЛЮЧЕВЫЕ РЕЗУЛЬТАТЫ АНАЛИЗА РОССИЙСКИХ БПИФ")
    print("="*60)
    
    # Общая информация
    metadata = report['report_metadata']
    print(f"\n📊 ОБЩАЯ ИНФОРМАЦИЯ:")
    print(f"   Проанализировано БПИФов: {metadata['total_etfs_analyzed']}")
    print(f"   Средняя качество данных: {metadata['average_data_quality']:.2f}")
    print(f"   Источники данных: {', '.join(metadata['data_sources_used'].keys())}")
    
    # Рыночные доли
    market_overview = report['market_overview']
    print(f"\n🏢 УПРАВЛЯЮЩИЕ КОМПАНИИ:")
    print(f"   Всего УК: {market_overview['management_companies']}")
    
    if market_overview['market_shares']:
        print("   Рыночные доли:")
        sorted_shares = sorted(market_overview['market_shares'].items(), 
                             key=lambda x: x[1] if x[1] else 0, reverse=True)
        for uk, share in sorted_shares[:5]:  # Топ-5
            if share:
                print(f"     • {uk}: {share:.1f}%")
    
    # Категории БПИФов
    print(f"\n📈 КАТЕГОРИИ БПИФ:")
    for category, count in market_overview['categories'].items():
        print(f"   • {category}: {count} фондов")
    
    # Анализ доходности
    performance = report['performance_analysis']
    if 'average_return' in performance:
        print(f"\n💰 ДОХОДНОСТЬ:")
        print(f"   Средняя доходность: {performance['average_return']:.2f}%")
        print(f"   Медианная доходность: {performance['median_return']:.2f}%")
        
        if 'best_performer' in performance:
            best = performance['best_performer']
            print(f"   🥇 Лучший: {best['ticker']} ({best['name']}) - {best['return']:.2f}%")
        
        if 'worst_performer' in performance:
            worst = performance['worst_performer']
            print(f"   📉 Худший: {worst['ticker']} ({worst['name']}) - {worst['return']:.2f}%")
        
        print(f"   Фондов с положительной доходностью: {performance['positive_returns_count']}")
        print(f"   Фондов с отрицательной доходностью: {performance['negative_returns_count']}")
    
    # Анализ рисков
    risk = report['risk_analysis']
    if 'average_volatility' in risk:
        print(f"\n⚠️ РИСКИ (ВОЛАТИЛЬНОСТЬ):")
        print(f"   Средняя волатильность: {risk['average_volatility']:.2f}%")
        
        if 'lowest_risk' in risk:
            low_risk = risk['lowest_risk']
            print(f"   🛡️ Наименее рискованный: {low_risk['ticker']} - {low_risk['volatility']:.2f}%")
        
        if 'highest_risk' in risk:
            high_risk = risk['highest_risk']
            print(f"   ⚡ Наиболее рискованный: {high_risk['ticker']} - {high_risk['volatility']:.2f}%")
    
    # Анализ ликвидности
    liquidity = report['liquidity_analysis']
    if 'average_daily_volume' in liquidity:
        print(f"\n💧 ЛИКВИДНОСТЬ:")
        print(f"   Средний дневной объем: {liquidity['average_daily_volume']:,.0f}")
        
        if 'most_liquid' in liquidity:
            liquid = liquidity['most_liquid']
            print(f"   🌊 Самый ликвидный: {liquid['ticker']} - {liquid['volume']:,.0f}")
    
    # Анализ комиссий
    costs = report['cost_analysis']
    if 'average_expense_ratio' in costs:
        print(f"\n💸 КОМИССИИ:")
        print(f"   Средняя комиссия: {costs['average_expense_ratio']:.2f}%")
        
        if 'lowest_cost' in costs:
            low_cost = costs['lowest_cost']
            print(f"   💰 Самый дешевый: {low_cost['ticker']} - {low_cost['expense_ratio']:.2f}%")
        
        if 'highest_cost' in costs:
            high_cost = costs['highest_cost']
            print(f"   💸 Самый дорогой: {high_cost['ticker']} - {high_cost['expense_ratio']:.2f}%")
    
    # Качество данных
    quality = report['data_quality_report']
    print(f"\n📊 КАЧЕСТВО ДАННЫХ:")
    print(f"   Записей с ценами: {quality['records_with_price_data']}/{quality['total_records']}")
    print(f"   Записей с доходностью: {quality['records_with_return_data']}/{quality['total_records']}")
    print(f"   Записей с объемами: {quality['records_with_volume_data']}/{quality['total_records']}")
    print(f"   Использование основного источника: {quality['fallback_usage']['primary_source_usage']}")
    print(f"   Использование резервных источников: {quality['fallback_usage']['secondary_source_usage']}")


def create_visualizations(df, timestamp):
    """Создание визуализаций"""
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Настройка стиля
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Создаем фигуру с подграфиками
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Анализ российских БПИФов', fontsize=16, fontweight='bold')
        
        # График 1: Доходность по БПИФам
        valid_returns = df[df['annual_return'].notna()]
        if len(valid_returns) > 0:
            axes[0, 0].bar(valid_returns['ticker'], valid_returns['annual_return'])
            axes[0, 0].set_title('Годовая доходность БПИФов (%)')
            axes[0, 0].set_xlabel('Тикер')
            axes[0, 0].set_ylabel('Доходность (%)')
            axes[0, 0].tick_params(axis='x', rotation=45)
        else:
            axes[0, 0].text(0.5, 0.5, 'Данные о доходности\nнедоступны', 
                           ha='center', va='center', transform=axes[0, 0].transAxes)
            axes[0, 0].set_title('Годовая доходность БПИФов (%)')
        
        # График 2: Рыночные доли УК
        valid_shares = df[df['market_share_percent'].notna()]
        if len(valid_shares) > 0:
            uk_shares = valid_shares.groupby('management_company')['market_share_percent'].first()
            axes[0, 1].pie(uk_shares.values, labels=uk_shares.index, autopct='%1.1f%%')
            axes[0, 1].set_title('Рыночные доли управляющих компаний')
        else:
            axes[0, 1].text(0.5, 0.5, 'Данные о рыночных\nдолях недоступны', 
                           ha='center', va='center', transform=axes[0, 1].transAxes)
            axes[0, 1].set_title('Рыночные доли управляющих компаний')
        
        # График 3: Волатильность vs Доходность
        valid_risk_return = df[(df['annual_return'].notna()) & (df['volatility'].notna())]
        if len(valid_risk_return) > 0:
            scatter = axes[1, 0].scatter(valid_risk_return['volatility'], 
                                       valid_risk_return['annual_return'],
                                       alpha=0.7, s=100)
            axes[1, 0].set_xlabel('Волатильность (%)')
            axes[1, 0].set_ylabel('Доходность (%)')
            axes[1, 0].set_title('Риск vs Доходность')
            
            # Добавляем подписи тикеров
            for idx, row in valid_risk_return.iterrows():
                axes[1, 0].annotate(row['ticker'], 
                                  (row['volatility'], row['annual_return']),
                                  xytext=(5, 5), textcoords='offset points', fontsize=8)
        else:
            axes[1, 0].text(0.5, 0.5, 'Данные о риске и\nдоходности недоступны', 
                           ha='center', va='center', transform=axes[1, 0].transAxes)
            axes[1, 0].set_title('Риск vs Доходность')
        
        # График 4: Комиссии по категориям
        valid_costs = df[df['expense_ratio'].notna()]
        if len(valid_costs) > 0:
            category_costs = valid_costs.groupby('category')['expense_ratio'].mean()
            axes[1, 1].bar(range(len(category_costs)), category_costs.values)
            axes[1, 1].set_title('Средние комиссии по категориям (%)')
            axes[1, 1].set_xlabel('Категория')
            axes[1, 1].set_ylabel('Комиссия (%)')
            axes[1, 1].set_xticks(range(len(category_costs)))
            axes[1, 1].set_xticklabels(category_costs.index, rotation=45, ha='right')
        else:
            axes[1, 1].text(0.5, 0.5, 'Данные о комиссиях\nнедоступны', 
                           ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Средние комиссии по категориям (%)')
        
        plt.tight_layout()
        
        # Сохраняем график
        chart_file = f'etf_analysis_charts_{timestamp}.png'
        plt.savefig(chart_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Визуализации сохранены в: {chart_file}")
        
    except ImportError:
        logger.warning("Matplotlib не установлен, визуализации пропущены")
    except Exception as e:
        logger.error(f"Ошибка создания визуализаций: {e}")


if __name__ == "__main__":
    main()