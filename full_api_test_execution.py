#!/usr/bin/env python3
"""
Полный скрипт для тестирования API и обновления документации
"""

import sys
import os
import subprocess
from datetime import datetime

def run_full_api_testing():
    """
    Выполняет полное тестирование API с обновлением всех файлов
    """
    
    print("🚀 ЗАПУСК ПОЛНОГО ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ")
    print("=" * 60)
    
    # Шаг 1: Запуск основного тестирования
    print("\n📡 ШАГ 1: Тестирование API источников...")
    try:
        # Добавляем путь к проекту
        sys.path.insert(0, '/Users/tumowuh/Desktop/market analysis')
        
        # Импортируем модули
        from api_research import APIResearcher
        from logger_config import logger
        
        # Создаем исследователь и запускаем тестирование
        researcher = APIResearcher()
        print("   Инициализация исследователя API...")
        
        results = researcher.run_comprehensive_research()
        print("   ✅ Тестирование API завершено")
        
        recommendations = researcher.generate_recommendations(results)
        print("   ✅ Рекомендации сгенерированы")
        
        # Выводим краткие результаты
        summary = results['summary']
        print(f"\n📊 КРАТКИЕ РЕЗУЛЬТАТЫ:")
        print(f"   Протестировано API: {results['total_apis_tested']}")
        print(f"   Активных: {summary['active_apis']}")
        print(f"   С ограничениями: {summary['limited_apis']}")
        print(f"   Недоступных: {summary['error_apis']}")
        print(f"   Успешность: {summary['success_rate']}%")
        
    except Exception as e:
        print(f"   ❌ Ошибка при тестировании API: {e}")
        return False
    
    # Шаг 2: Создание визуализации (если возможно)
    print("\n📊 ШАГ 2: Создание визуализации...")
    try:
        import matplotlib.pyplot as plt
        
        # Простая визуализация результатов
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
        # График статусов API
        status_counts = {
            'Активные': summary['active_apis'],
            'Ограниченные': summary['limited_apis'],
            'Недоступные': summary['error_apis']
        }
        
        colors = ['#2ecc71', '#f39c12', '#e74c3c']
        bars = ax.bar(status_counts.keys(), status_counts.values(), color=colors)
        
        ax.set_title('Результаты тестирования API источников', fontsize=14, fontweight='bold')
        ax.set_ylabel('Количество API')
        
        # Добавляем значения на столбцы
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Сохранение
        output_path = '/Users/tumowuh/Desktop/market analysis/api_test_results_simple.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"   ✅ Визуализация сохранена: api_test_results_simple.png")
        
        plt.close()
        
    except Exception as e:
        print(f"   ⚠️ Визуализация недоступна: {e}")
    
    # Шаг 3: Создание отчета
    print("\n📝 ШАГ 3: Создание детального отчета...")
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f'/Users/tumowuh/Desktop/market analysis/api_testing_summary_{timestamp}.md'
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ API ИСТОЧНИКОВ\n\n")
            f.write(f"*Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}*\n\n")
            
            f.write("## 📊 ОБЩАЯ СТАТИСТИКА\n\n")
            f.write(f"- Протестировано API: **{results['total_apis_tested']}**\n")
            f.write(f"- Активных: **{summary['active_apis']}**\n")
            f.write(f"- С ограничениями: **{summary['limited_apis']}**\n")
            f.write(f"- Недоступных: **{summary['error_apis']}**\n")
            f.write(f"- Успешность: **{summary['success_rate']}%**\n\n")
            
            f.write("## 🎯 РЕКОМЕНДАЦИИ\n\n")
            
            if recommendations['primary_sources']:
                f.write("### ✅ ОСНОВНЫЕ ИСТОЧНИКИ\n\n")
                for source in recommendations['primary_sources']:
                    f.write(f"- **{source['name']}**\n")
                f.write("\n")
            
            if recommendations['secondary_sources']:
                f.write("### 🔶 ДОПОЛНИТЕЛЬНЫЕ ИСТОЧНИКИ\n\n")
                for source in recommendations['secondary_sources']:
                    f.write(f"- **{source['name']}** *(доступ: {source['access_method']})*\n")
                f.write("\n")
            
            f.write("## 📋 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ\n\n")
            for api_name, api_data in results['apis'].items():
                status_emoji = "✅" if api_data['status'] == 'active' else "🔶" if api_data['status'] in ['limited_public', 'requires_token'] else "❌"
                f.write(f"### {status_emoji} {api_data['name']}\n")
                f.write(f"- **Статус:** {api_data['status']}\n")
                f.write(f"- **URL:** {api_data.get('base_url', 'N/A')}\n")
                if 'error' in api_data:
                    f.write(f"- **Ошибка:** {api_data['error']}\n")
                f.write("\n")
        
        print(f"   ✅ Отчет сохранен: {report_path}")
        
    except Exception as e:
        print(f"   ❌ Ошибка создания отчета: {e}")
    
    # Шаг 4: Обновление tasks.md
    print("\n📋 ШАГ 4: Обновление файла задач...")
    try:
        from update_tasks import update_tasks_with_api_testing
        success = update_tasks_with_api_testing()
        
        if success:
            print("   ✅ Файл tasks.md обновлен")
        else:
            print("   ⚠️ Создан отдельный файл с обновлениями")
            
    except Exception as e:
        print(f"   ❌ Ошибка обновления tasks.md: {e}")
    
    # Финальный вывод
    print("\n" + "="*60)
    print("🎉 ТЕСТИРОВАНИЕ API ЗАВЕРШЕНО!")
    print("="*60)
    
    print(f"\n📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print(f"   • Успешность тестирования: {summary['success_rate']}%")
    print(f"   • Рекомендуемых источников: {len(recommendations['primary_sources'])}")
    print(f"   • Дополнительных источников: {len(recommendations['secondary_sources'])}")
    
    print(f"\n📁 СОЗДАННЫЕ ФАЙЛЫ:")
    print(f"   • api_testing_summary_{timestamp}.md - сводный отчет")
    if os.path.exists('/Users/tumowuh/Desktop/market analysis/api_test_results_simple.png'):
        print(f"   • api_test_results_simple.png - визуализация")
    print(f"   • api_research_results_{timestamp}.json - детальные данные")
    print(f"   • tasks.md - обновлен статус задач")
    
    print(f"\n🚀 СЛЕДУЮЩИЕ ШАГИ:")
    print(f"   1. Изучить детальный отчет для выбора API к интеграции")
    print(f"   2. Настроить выбранные API источники")
    print(f"   3. Реализовать fallback механизм")
    print(f"   4. Протестировать интеграцию на реальных данных")
    
    return True

if __name__ == "__main__":
    run_full_api_testing()