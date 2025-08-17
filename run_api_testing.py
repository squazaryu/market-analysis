"""
Запуск тестирования API источников данных с анализом результатов
"""

import sys
import os
sys.path.append('/Users/tumowuh/Desktop/market analysis')

from api_research import APIResearcher
from logger_config import logger
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def create_api_testing_visualization(results, recommendations):
    """
    Создание визуализации результатов тестирования API
    """
    # Настройка matplotlib для русского текста
    plt.rcParams['font.family'] = ['Arial Unicode MS', 'DejaVu Sans', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ ДАННЫХ',
                 fontsize=16, fontweight='bold', y=0.98)
    
    # 1. Статус API по категориям
    ax1 = axes[0, 0]
    status_counts = {}
    
    for api_name, api_data in results['apis'].items():
        status = api_data['status']
        if status == 'active':
            category = 'Активные'
        elif status in ['limited_public', 'requires_token', 'web_interface_available']:
            category = 'Ограниченные'
        else:
            category = 'Недоступные'
        
        status_counts[category] = status_counts.get(category, 0) + 1
    
    colors = ['#2ecc71', '#f39c12', '#e74c3c']
    wedges, texts, autotexts = ax1.pie(status_counts.values(),
                                      labels=status_counts.keys(),
                                      autopct='%1.1f%%',
                                      colors=colors,
                                      startangle=90)
    ax1.set_title('Доступность API источников', fontweight='bold')
    
    # 2. Качество данных по API
    ax2 = axes[0, 1]
    api_names = []
    quality_scores = []
    
    for api_name, api_data in results['apis'].items():
        if api_data['status'] not in ['error', 'critical_error']:
            api_names.append(api_name.upper())
            
            # Рассчитываем оценку качества на основе возможностей
            capabilities = api_data.get('capabilities', {})
            if isinstance(capabilities, dict):
                score = len([v for v in capabilities.values() if v is True]) * 10
                quality_scores.append(min(score, 100))
            else:
                quality_scores.append(50)
    
    if api_names and quality_scores:
        bars = ax2.barh(api_names, quality_scores,
                       color=plt.cm.RdYlGn([q/100 for q in quality_scores]))
        ax2.set_xlabel('Оценка качества API (%)')
        ax2.set_title('Качество API источников', fontweight='bold')
        ax2.set_xlim(0, 100)
        
        # Добавляем значения на столбцы
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax2.text(width + 2, bar.get_y() + bar.get_height()/2,
                    f'{int(width)}%', ha='left', va='center')
    
    # 3. Рекомендации по использованию
    ax3 = axes[1, 0]
    
    recommendation_data = {
        'Основные источники': len(recommendations['primary_sources']),
        'Дополнительные источники': len(recommendations['secondary_sources']),
        'Избегать': len(recommendations['avoid'])
    }
    
    bars = ax3.bar(recommendation_data.keys(), recommendation_data.values(),
                   color=['#2ecc71', '#3498db', '#e74c3c'])
    ax3.set_title('Рекомендации по использованию', fontweight='bold')
    ax3.set_ylabel('Количество API')
    
    # Добавляем значения на столбцы
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')
    
    # 4. Покрытие данных
    ax4 = axes[1, 1]
    
    data_types = {
        'Торговые данные': 0,
        'Исторические данные': 0,
        'Реал-тайм котировки': 0,
        'Макроэкономика': 0,
        'Фундаментальные данные': 0
    }
    
    for api_name, api_data in results['apis'].items():
        if api_data['status'] in ['active', 'limited_public', 'requires_token']:
            capabilities = api_data.get('capabilities', {})
            
            if capabilities.get('trading_volumes') or capabilities.get('historical_data'):
                data_types['Торговые данные'] += 1
            if capabilities.get('historical_data') or capabilities.get('historical_candles'):
                data_types['Исторические данные'] += 1
            if capabilities.get('real_time_data') or capabilities.get('real_time_quotes'):
                data_types['Реал-тайм котировки'] += 1
            if capabilities.get('currency_rates') or capabilities.get('economic_data'):
                data_types['Макроэкономика'] += 1
            if capabilities.get('fundamental_data'):
                data_types['Фундаментальные данные'] += 1
    
    bars = ax4.bar(range(len(data_types)), list(data_types.values()),
                   color=plt.cm.viridis([i/len(data_types) for i in range(len(data_types))]))
    ax4.set_xticks(range(len(data_types)))
    ax4.set_xticklabels([key.replace(' ', '\n') for key in data_types.keys()],
                       rotation=0, fontsize=9)
    ax4.set_title('Покрытие типов данных', fontweight='bold')
    ax4.set_ylabel('Количество API')
    
    # Добавляем значения на столбцы
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    
    # Сохранение графика
    output_path = '/Users/tumowuh/Desktop/market analysis/api_testing_results.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    logger.info(f"Визуализация результатов сохранена: {output_path}")
    
    plt.show()
    return fig

def generate_detailed_report(results, recommendations):
    """
    Генерация детального отчета по результатам тестирования
    """
    report_lines = []
    report_lines.append("# РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ ДАННЫХ")
    report_lines.append(f"*Дата тестирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}*")
    report_lines.append("")
    
    # Общая статистика
    summary = results['summary']
    report_lines.append("## 📊 ОБЩАЯ СТАТИСТИКА")
    report_lines.append("")
    report_lines.append(f"- **Протестировано API:** {results['total_apis_tested']}")
    report_lines.append(f"- **Активных:** {summary['active_apis']}")
    report_lines.append(f"- **С ограничениями:** {summary['limited_apis']}")
    report_lines.append(f"- **Недоступных:** {summary['error_apis']}")
    report_lines.append(f"- **Успешность:** {summary['success_rate']}%")
    report_lines.append("")
    
    # Детальные результаты по каждому API
    report_lines.append("## 🔍 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ")
    report_lines.append("")
    
    for api_name, api_data in results['apis'].items():
        report_lines.append(f"### {api_data['name']}")
        report_lines.append("")
        report_lines.append(f"**Статус:** {api_data['status']}")
        report_lines.append(f"**URL:** {api_data.get('base_url', 'N/A')}")
        report_lines.append(f"**Качество данных:** {api_data.get('data_quality', 'unknown')}")
        report_lines.append("")
        
        # Возможности
        capabilities = api_data.get('capabilities', {})
        if capabilities:
            report_lines.append("**Возможности:**")
            for key, value in capabilities.items():
                if value is True:
                    report_lines.append(f"- ✅ {key.replace('_', ' ').title()}")
                elif value not in [False, None]:
                    report_lines.append(f"- 📊 {key.replace('_', ' ').title()}: {value}")
            report_lines.append("")
        
        # Ограничения
        limitations = api_data.get('limitations', {})
        if limitations:
            report_lines.append("**Ограничения:**")
            for key, value in limitations.items():
                if value is True:
                    report_lines.append(f"- ⚠️ {key.replace('_', ' ').title()}")
                elif value not in [False, None]:
                    report_lines.append(f"- 📝 {key.replace('_', ' ').title()}: {value}")
            report_lines.append("")
        
        # Ошибки
        if 'error' in api_data:
            report_lines.append(f"**Ошибка:** {api_data['error']}")
            report_lines.append("")
        
        report_lines.append("---")
        report_lines.append("")
    
    # Рекомендации
    report_lines.append("## 🎯 РЕКОМЕНДАЦИИ ПО ИНТЕГРАЦИИ")
    report_lines.append("")
    
    # Основные источники
    if recommendations['primary_sources']:
        report_lines.append("### ✅ РЕКОМЕНДУЕМЫЕ К ИСПОЛЬЗОВАНИЮ")
        report_lines.append("")
        for source in recommendations['primary_sources']:
            report_lines.append(f"**{source['name']}**")
            strengths = source.get('strengths', {})
            if strengths:
                report_lines.append("- Преимущества:")
                for key, value in strengths.items():
                    if value is True:
                        report_lines.append(f"  - {key.replace('_', ' ').title()}")
            report_lines.append("")
    
    # Дополнительные источники
    if recommendations['secondary_sources']:
        report_lines.append("### 🔶 ДОПОЛНИТЕЛЬНЫЕ ИСТОЧНИКИ")
        report_lines.append("")
        for source in recommendations['secondary_sources']:
            report_lines.append(f"**{source['name']}** *(Доступ: {source['access_method']})*")
            potential = source.get('potential', {})
            if potential:
                report_lines.append("- Потенциал:")
                for key, value in potential.items():
                    if value is True:
                        report_lines.append(f"  - {key.replace('_', ' ').title()}")
            report_lines.append("")
    
    # Избегать
    if recommendations['avoid']:
        report_lines.append("### ❌ НЕ РЕКОМЕНДУЕМЫЕ")
        report_lines.append("")
        for source in recommendations['avoid']:
            report_lines.append(f"**{source['name']}** - {source['reason']}")
        report_lines.append("")
    
    # Сохранение отчета
    report_path = f"/Users/tumowuh/Desktop/market analysis/api_testing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"Детальный отчет сохранен: {report_path}")
    return report_path

def print_summary_to_console(results, recommendations):
    """
    Вывод краткого резюме в консоль
    """
    print("\n" + "="*80)
    print("🔍 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ ДАННЫХ")
    print("="*80)
    
    # Общая статистика
    summary = results['summary']
    print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Протестировано API: {results['total_apis_tested']}")
    print(f"   Активных: {summary['active_apis']}")
    print(f"   С ограничениями: {summary['limited_apis']}")
    print(f"   Недоступных: {summary['error_apis']}")
    print(f"   Успешность: {summary['success_rate']}%")
    
    # Статус каждого API
    print(f"\n🔍 СТАТУС API:")
    for api_name, api_data in results['apis'].items():
        status = api_data['status']
        name = api_data['name']
        
        if status == 'active':
            emoji = "✅"
        elif status in ['limited_public', 'requires_token', 'web_interface_available']:
            emoji = "🔶"
        else:
            emoji = "❌"
        
        print(f"   {emoji} {name:<30} - {status}")
    
    # Рекомендации
    print(f"\n🎯 РЕКОМЕНДАЦИИ:")
    
    if recommendations['primary_sources']:
        print(f"   ✅ Основные источники ({len(recommendations['primary_sources'])}):")
        for source in recommendations['primary_sources']:
            print(f"      - {source['name']}")
    
    if recommendations['secondary_sources']:
        print(f"   🔶 Дополнительные источники ({len(recommendations['secondary_sources'])}):")
        for source in recommendations['secondary_sources']:
            print(f"      - {source['name']} ({source['access_method']})")
    
    if recommendations['avoid']:
        print(f"   ❌ Избегать ({len(recommendations['avoid'])}):")
        for source in recommendations['avoid']:
            print(f"      - {source['name']}")
    
    print("\n" + "="*80)

def main():
    """
    Основная функция для запуска тестирования API
    """
    print("🚀 ЗАПУСК ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ ДАННЫХ...")
    print("=" * 60)
    
    try:
        # Создаем исследователь API
        researcher = APIResearcher()
        
        # Запускаем комплексное исследование
        print("📡 Тестирование API источников...")
        results = researcher.run_comprehensive_research()
        
        print("🧠 Генерация рекомендаций...")
        recommendations = researcher.generate_recommendations(results)
        
        # Выводим краткое резюме в консоль
        print_summary_to_console(results, recommendations)
        
        # Создаем визуализацию
        print("📊 Создание визуализации результатов...")
        create_api_testing_visualization(results, recommendations)
        
        # Генерируем детальный отчет
        print("📝 Генерация детального отчета...")
        report_path = generate_detailed_report(results, recommendations)
        
        print(f"\n✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО УСПЕШНО!")
        print(f"📊 Визуализация: api_testing_results.png")
        print(f"📝 Отчет: {report_path}")
        print(f"📈 JSON результаты сохранены в директории проекта")
        
        return results, recommendations
        
    except Exception as e:
        logger.error(f"Критическая ошибка при тестировании API: {e}", exc_info=True)
        print(f"❌ Ошибка: {e}")
        return None, None

if __name__ == "__main__":
    main()