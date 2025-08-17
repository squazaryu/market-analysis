#!/usr/bin/env python3
"""
Скрипт для запуска веб-дашборда ETF аналитики
"""

import sys
import webbrowser
import time
from threading import Timer
from web_dashboard import app

def open_browser():
    """Открывает браузер через 2 секунды после запуска сервера"""
    webbrowser.open('http://localhost:5000')

def main():
    """Основная функция запуска"""
    
    print("🚀 Запуск веб-дашборда ETF аналитики...")
    print("=" * 60)
    print("📊 Загрузка данных и инициализация...")
    
    # Проверяем, что данные загружены
    from web_dashboard import dashboard
    if dashboard.enhanced_df is None:
        print("❌ Ошибка: Данные ETF не загружены!")
        print("💡 Убедитесь, что файлы данных присутствуют в директории")
        sys.exit(1)
    
    print(f"✅ Загружено {len(dashboard.enhanced_df)} ETF")
    print("=" * 60)
    print("🌐 Веб-дашборд будет доступен по адресу:")
    print("   http://localhost:5000")
    print("=" * 60)
    print("📋 Возможности дашборда:")
    print("   • Интерактивные графики риск-доходность")
    print("   • Секторальный анализ по категориям")
    print("   • Корреляционная матрица ETF")
    print("   • Эффективная граница портфеля")
    print("   • Инвестиционные рекомендации")
    print("   • Таблица с данными ETF")
    print("=" * 60)
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("🔄 Данные обновляются автоматически каждые 5 минут")
    print("=" * 60)
    
    # Открываем браузер через 2 секунды
    Timer(2.0, open_browser).start()
    
    try:
        # Запускаем Flask приложение
        app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("\n🛑 Веб-дашборд остановлен пользователем")
        print("👋 До свидания!")
    except Exception as e:
        print(f"\n❌ Ошибка запуска веб-дашборда: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()