#!/usr/bin/env python3
"""
Запуск планировщика автоматического обновления данных
Использование: python3 start_scheduler.py
"""

from data_scheduler import DataScheduler

if __name__ == "__main__":
    print("🚀 Запуск планировщика автоматического обновления данных БПИФ")
    print("="*60)
    
    scheduler = DataScheduler()
    scheduler.setup_schedule()
    
    print("📅 Настроенное расписание:")
    print("   • InvestFunds.ru: ежедневно в 10:00 (все 96 фондов)")
    print("   • MOEX данные: еженедельно в пятницу 09:00")
    print()
    print("📦 Данные автоматически архивируются перед обновлением")
    print("📊 Логи сохраняются в папку logs/")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("="*60)
    
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        print("\n👋 Планировщик остановлен пользователем")
        print("✅ Все данные сохранены")