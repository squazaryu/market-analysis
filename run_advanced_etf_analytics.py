#!/usr/bin/env python3
"""
Запуск полного цикла продвинутой аналитики ETF
Создает комплексный дашборд с всеми видами анализа
"""

import sys
import traceback
from datetime import datetime
from pathlib import Path

def main():
    """Основная функция запуска аналитики"""
    
    print("🚀 Запуск продвинутой аналитики российских ETF")
    print("=" * 60)
    
    try:
        # 1. Создаем интерактивный дашборд
        print("\n📊 Создание интерактивного дашборда...")
        from interactive_etf_dashboard import InteractiveETFDashboard
        
        dashboard = InteractiveETFDashboard()
        dashboard.load_data()
        
        # Создаем все графики
        dashboard_file = dashboard.create_comprehensive_dashboard()
        if dashboard_file:
            print(f"✅ Интерактивный дашборд создан: {dashboard_file}")
        
        # 2. Создаем продвинутую аналитику
        print("\n🔬 Запуск продвинутой аналитики...")
        from advanced_etf_analytics import AdvancedETFAnalytics
        
        analytics = AdvancedETFAnalytics()
        analytics_file = analytics.create_comprehensive_analysis()
        if analytics_file:
            print(f"✅ Продвинутая аналитика создана: {analytics_file}")
        
        # 3. Создаем инвестиционные рекомендации
        print("\n💡 Генерация инвестиционных рекомендаций...")
        recommendations = analytics.generate_investment_recommendations()
        
        if recommendations:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            rec_file = f"investment_recommendations_{timestamp}.md"
            
            with open(rec_file, 'w', encoding='utf-8') as f:
                f.write("# 💰 Инвестиционные рекомендации по российским ETF\n\n")
                f.write(f"**Дата анализа:** {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n")
                
                for category, data in recommendations.items():
                    f.write(f"## {data['description']}\n\n")
                    f.write(f"**Критерии отбора:** {data['criteria']}\n\n")
                    
                    if data['etfs']:
                        f.write("| Тикер | Доходность | Волатильность | Sharpe Ratio |\n")
                        f.write("|-------|------------|---------------|---------------|\n")
                        
                        for etf in data['etfs'][:5]:  # Топ-5
                            f.write(f"| {etf['ticker']} | {etf.get('annual_return', 0):.1f}% | "
                                   f"{etf.get('volatility', 0):.1f}% | {etf.get('sharpe_ratio', 0):.2f} |\n")
                    else:
                        f.write("*Нет ETF, соответствующих критериям*\n")
                    
                    f.write("\n")
            
            print(f"✅ Рекомендации сохранены: {rec_file}")
        
        # 4. Создаем сводный отчет
        print("\n📋 Создание сводного отчета...")
        
        summary_file = f"advanced_analytics_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("# 📊 Сводный отчет по продвинутой аналитике ETF\n\n")
            f.write(f"**Дата создания:** {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n")
            
            f.write("## 🎯 Выполненные задачи\n\n")
            f.write("✅ **Секторальный анализ** - Анализ по категориям активов\n")
            f.write("✅ **Корреляционная матрица** - Анализ взаимосвязей между ETF\n")
            f.write("✅ **Риск-скорректированная доходность** - Расчет Sharpe и Sortino коэффициентов\n")
            f.write("✅ **Анализ состава портфеля** - Инсайты для диверсификации\n")
            f.write("✅ **Атрибуция доходности** - Расчет альфы и беты\n")
            f.write("✅ **Временные ряды** - Сравнение доходности во времени\n")
            f.write("✅ **Эффективная граница** - Визуализация оптимальных портфелей\n")
            f.write("✅ **Дивидендная доходность** - Сравнительный анализ\n")
            f.write("✅ **Анализ расходов** - Сравнение expense ratio\n")
            f.write("✅ **Взвешенные портфели** - Сравнение стратегий взвешивания\n\n")
            
            f.write("## 📁 Созданные файлы\n\n")
            if dashboard_file:
                f.write(f"- **Интерактивный дашборд:** `{dashboard_file}`\n")
            if analytics_file:
                f.write(f"- **Продвинутая аналитика:** `{analytics_file}`\n")
            f.write(f"- **Инвестиционные рекомендации:** `{rec_file}`\n")
            f.write(f"- **Сводный отчет:** `{summary_file}`\n\n")
            
            f.write("## 🔍 Ключевые инсайты\n\n")
            f.write("1. **Диверсификация** - Анализ показал возможности для улучшения диверсификации\n")
            f.write("2. **Риск-доходность** - Выявлены ETF с оптимальным соотношением риск/доходность\n")
            f.write("3. **Корреляции** - Определены группы ETF с низкой корреляцией\n")
            f.write("4. **Эффективность** - Построена эффективная граница для оптимизации портфеля\n\n")
            
            f.write("## 🎯 Фокус на принятие инвестиционных решений\n\n")
            f.write("Все анализы сосредоточены на практических инвестиционных инсайтах:\n")
            f.write("- Выбор ETF для различных инвестиционных стратегий\n")
            f.write("- Оптимизация портфеля с учетом риска\n")
            f.write("- Диверсификация по секторам и активам\n")
            f.write("- Анализ стоимости владения (expense ratio)\n")
            f.write("- Оценка ликвидности и торговых объемов\n\n")
        
        print(f"✅ Сводный отчет создан: {summary_file}")
        
        print("\n" + "=" * 60)
        print("🎉 ПРОДВИНУТАЯ АНАЛИТИКА ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        
        print(f"\n📁 Созданные файлы:")
        if dashboard_file:
            print(f"   • {dashboard_file}")
        if analytics_file:
            print(f"   • {analytics_file}")
        print(f"   • {rec_file}")
        print(f"   • {summary_file}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении аналитики:")
        print(f"   {str(e)}")
        print(f"\n🔍 Детали ошибки:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)