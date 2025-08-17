#!/usr/bin/env python3
"""
Простой запуск тестирования API без дополнительных зависимостей
"""

import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, '/Users/tumowuh/Desktop/market analysis')

# Импортируем и запускаем
try:
    from run_api_testing import main
    
    print("🔍 НАЧИНАЕМ ТЕСТИРОВАНИЕ API ИСТОЧНИКОВ...")
    print("-" * 50)
    
    results, recommendations = main()
    
    if results and recommendations:
        print("\n🎉 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
        print(f"✅ Успешность: {results['summary']['success_rate']}%")
        print(f"📊 Активных API: {results['summary']['active_apis']}")
        print(f"🔶 С ограничениями: {results['summary']['limited_apis']}")
        print(f"❌ Недоступных: {results['summary']['error_apis']}")
        
        print(f"\n📋 КЛЮЧЕВЫЕ РЕКОМЕНДАЦИИ:")
        
        if recommendations['primary_sources']:
            print(f"🥇 Основные источники:")
            for source in recommendations['primary_sources']:
                print(f"   • {source['name']}")
        
        if recommendations['secondary_sources']:
            print(f"🥈 Дополнительные источники:")
            for source in recommendations['secondary_sources']:
                print(f"   • {source['name']}")
        
        print(f"\n📁 Результаты сохранены в файлах:")
        print(f"   • api_testing_results.png - визуализация")
        print(f"   • api_testing_report_*.md - детальный отчет")
        print(f"   • api_research_results_*.json - сырые данные")
        
    else:
        print("❌ Тестирование завершилось с ошибками")
        
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что все необходимые файлы находятся в директории проекта")
    
except Exception as e:
    print(f"❌ Неожиданная ошибка: {e}")
    import traceback
    traceback.print_exc()