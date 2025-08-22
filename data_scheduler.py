#!/usr/bin/env python3
"""
Планировщик автоматического обновления данных ETF/БПИФ
- MOEX данные: еженедельно в пятницу
- investfunds.ru данные: ежедневно в 10:00
"""

import schedule
import time
import logging
import threading
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import json

class DataScheduler:
    def __init__(self):
        self.setup_logging()
        self.status_file = Path("scheduler_status.json")
        self.running = False
        
    def setup_logging(self):
        """Настройка логирования для планировщика"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'scheduler_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('DataScheduler')
        
    def save_status(self, task_type: str, status: str, details: str = ""):
        """Сохранение статуса выполнения задач"""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = {}
            
            data[task_type] = {
                'last_run': datetime.now().isoformat(),
                'status': status,
                'details': details
            }
            
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения статуса: {e}")
    
    def update_investfunds_data(self):
        """Ежедневное обновление данных investfunds.ru"""
        self.logger.info("🔄 Начало ежедневного обновления investfunds данных...")
        
        try:
            # Импортируем и запускаем функцию обновления данных
            from simple_dashboard import create_initial_data
            from investfunds_parser import InvestFundsParser
            
            # Архивируем текущий кэш
            parser = InvestFundsParser()
            cache_archived = parser.archive_cache()
            self.logger.info(f"📦 Кэш заархивирован: {cache_archived}")
            
            # Создаем новые данные со всеми 96 фондами
            start_time = datetime.now()
            success = create_initial_data()
            end_time = datetime.now()
            
            duration = (end_time - start_time).total_seconds()
            
            if success:
                self.logger.info(f"✅ Данные investfunds обновлены успешно за {duration:.1f} сек")
                self.save_status('investfunds_daily', 'success', f'Обновление завершено за {duration:.1f} сек')
                
                # Получаем статистику архива
                summary = parser.get_fund_history_summary()
                self.logger.info(f"📊 Архив: {summary['archives']} папок, {summary['funds']} фондов, {summary['total_files']} файлов")
                
            else:
                self.logger.error("❌ Не удалось обновить данные investfunds")
                self.save_status('investfunds_daily', 'error', 'Не удалось создать данные')
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления investfunds: {e}")
            self.save_status('investfunds_daily', 'error', str(e))
    
    def update_moex_data(self):
        """Еженедельное обновление данных MOEX (пятница)"""
        self.logger.info("📈 Начало еженедельного обновления MOEX данных...")
        
        try:
            # Здесь можно добавить специфическое обновление MOEX данных
            # Пока используем тот же механизм, но можно расширить
            from moex_provider import MOEXDataProvider
            
            moex = MOEXDataProvider()
            etfs = moex.get_securities_list()
            
            self.logger.info(f"📊 Получено {len(etfs)} ETF с MOEX")
            self.save_status('moex_weekly', 'success', f'Получено {len(etfs)} ETF тикеров')
            
            # Также обновляем investfunds данные в пятницу
            self.update_investfunds_data()
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления MOEX: {e}")
            self.save_status('moex_weekly', 'error', str(e))
    
    def get_status(self):
        """Получение статуса последних обновлений"""
        try:
            if not self.status_file.exists():
                return {'message': 'Статус не найден - обновления еще не выполнялись'}
            
            with open(self.status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            return {'error': f'Ошибка чтения статуса: {e}'}
    
    def setup_schedule(self):
        """Настройка расписания задач"""
        
        # Ежедневное обновление investfunds в 10:00
        schedule.every().day.at("10:00").do(self.update_investfunds_data)
        self.logger.info("📅 Настроено ежедневное обновление investfunds в 10:00")
        
        # Еженедельное обновление MOEX в пятницу в 09:00
        schedule.every().friday.at("09:00").do(self.update_moex_data)
        self.logger.info("📅 Настроено еженедельное обновление MOEX в пятницу 09:00")
        
        # Тестовое обновление через 1 минуту (можно удалить в продакшене)
        # schedule.every(1).minutes.do(self.update_investfunds_data)
        # self.logger.info("🧪 Настроено тестовое обновление через 1 минуту")
    
    def run_scheduler(self):
        """Запуск планировщика в фоновом режиме"""
        self.running = True
        self.logger.info("🚀 Планировщик запущен")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
            except KeyboardInterrupt:
                self.logger.info("⏹️ Планировщик остановлен пользователем")
                break
            except Exception as e:
                self.logger.error(f"❌ Ошибка в планировщике: {e}")
                time.sleep(60)
    
    def start_background(self):
        """Запуск планировщика в отдельном потоке"""
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()
        self.logger.info("📱 Планировщик запущен в фоновом режиме")
        return scheduler_thread
    
    def stop(self):
        """Остановка планировщика"""
        self.running = False
        self.logger.info("⏹️ Планировщик остановлен")

def main():
    """Основная функция для запуска планировщика"""
    scheduler = DataScheduler()
    scheduler.setup_schedule()
    
    print("🤖 Планировщик автоматического обновления данных")
    print("📅 Расписание:")
    print("   • Investfunds.ru: ежедневно в 10:00")
    print("   • MOEX: еженедельно в пятницу 09:00")
    print("⏹️  Для остановки нажмите Ctrl+C")
    
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        print("\n👋 Планировщик остановлен")

if __name__ == "__main__":
    main()