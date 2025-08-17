#!/usr/bin/env python3
"""
Скрипт быстрого запуска дашборда анализа БПИФ
Автоматически устанавливает зависимости и запускает дашборд
"""

import sys
import subprocess
import os
from pathlib import Path

def check_dependencies():
    """Проверяет и устанавливает зависимости"""
    try:
        import flask, pandas, requests, beautifulsoup4, plotly
        print("✅ Все зависимости уже установлены")
        return True
    except ImportError as e:
        print(f"📦 Обнаружены отсутствующие зависимости: {e}")
        print("🔄 Устанавливаем зависимости...")
        
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            print("✅ Зависимости установлены успешно")
            return True
        except subprocess.CalledProcessError:
            print("❌ Ошибка установки зависимостей")
            print("💡 Попробуйте вручную: pip install -r requirements.txt")
            return False

def check_data_files():
    """Проверяет наличие файлов данных"""
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if data_files:
        latest = max(data_files, key=lambda x: x.stat().st_mtime)
        print(f"📊 Найден файл данных: {latest}")
        return True
    else:
        print("📥 Файлы данных не найдены - будут созданы автоматически при первом запуске")
        return False

def main():
    print("🚀 ЗАПУСК ДАШБОРДА АНАЛИЗА РОССИЙСКИХ БПИФ")
    print("=" * 60)
    
    # Проверяем зависимости
    if not check_dependencies():
        return
    
    # Проверяем данные
    check_data_files()
    
    print("\n🌐 Запуск веб-дашборда...")
    print("📍 Адрес: http://localhost:5004")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("=" * 60)
    
    # Запускаем дашборд
    try:
        subprocess.call([sys.executable, "simple_dashboard.py"])
    except KeyboardInterrupt:
        print("\n👋 Дашборд остановлен")
    except Exception as e:
        print(f"\n❌ Ошибка запуска: {e}")
        print("💡 Попробуйте: python simple_dashboard.py")

if __name__ == "__main__":
    main()