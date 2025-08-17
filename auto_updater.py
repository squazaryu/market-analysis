#!/usr/bin/env python3
"""
Система автоматического обновления данных БПИФ
Еженедельное обновление с сохранением исторических снимков
"""

import os
import sys
import subprocess
import schedule
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import json
from typing import Dict, List
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

class AutoUpdater:
    """Система автоматического обновления данных БПИФ"""
    
    def __init__(self, project_dir: str = "/Users/tumowuh/Desktop/market analysis"):
        self.project_dir = Path(project_dir)
        self.backup_dir = self.project_dir / "backups"
        self.logs_dir = self.project_dir / "logs"
        self.config_file = self.project_dir / "update_config.json"
        
        # Создаем необходимые директории
        self.backup_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Настройка логирования
        self.logger = self._setup_logger()
        
        # Загружаем конфигурацию
        self.config = self._load_config()
    
    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования для автообновления"""
        
        logger = logging.getLogger('AutoUpdater')
        logger.setLevel(logging.INFO)
        
        # Удаляем существующие обработчики
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Файловый обработчик
        log_file = self.logs_dir / f"auto_update_{datetime.now().strftime('%Y%m')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Форматтер
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _load_config(self) -> Dict:
        """Загружает конфигурацию автообновления"""
        
        default_config = {
            "update_schedule": {
                "day_of_week": "sunday",  # Воскресенье
                "time": "02:00",          # 02:00 утра
                "timezone": "Europe/Moscow"
            },
            "backup_settings": {
                "keep_backups": 12,       # Хранить 12 недель = 3 месяца
                "compress_old": True      # Сжимать старые бэкапы
            },
            "notification": {
                "enabled": False,
                "email": "",
                "smtp_server": "",
                "smtp_port": 587
            },
            "data_collectors": [
                "enhanced_real_data_collector.py",
                "full_moex_etf_collector.py"
            ],
            "post_update_scripts": [
                # Скрипты, которые запускаются после обновления данных
            ]
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Дополняем недостающими значениями по умолчанию
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            except Exception as e:
                self.logger.warning(f"Ошибка загрузки конфигурации: {e}. Используем настройки по умолчанию.")
        
        # Сохраняем конфигурацию по умолчанию
        self._save_config(default_config)
        return default_config
    
    def _save_config(self, config: Dict):
        """Сохраняет конфигурацию"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения конфигурации: {e}")
    
    def create_backup(self) -> str:
        """Создает бэкап текущих данных"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"etf_data_backup_{timestamp}"
        backup_path = self.backup_dir / backup_name
        
        try:
            self.logger.info(f"Создание бэкапа: {backup_name}")
            
            # Создаем директорию бэкапа
            backup_path.mkdir(exist_ok=True)
            
            # Копируем важные файлы данных
            data_patterns = [
                'enhanced_etf_data_*.csv',
                'full_moex_etf_data_*.csv',
                'historical_etf_data_*.csv',
                'real_*.csv'
            ]
            
            backed_up_files = 0
            for pattern in data_patterns:
                for file_path in self.project_dir.glob(pattern):
                    if file_path.is_file():
                        shutil.copy2(file_path, backup_path)
                        backed_up_files += 1
            
            # Копируем важные отчеты
            report_patterns = [
                '*_report_*.json',
                'investment_recommendations*.md',
                'advanced_analytics_summary*.md'
            ]
            
            for pattern in report_patterns:
                for file_path in self.project_dir.glob(pattern):
                    if file_path.is_file():
                        shutil.copy2(file_path, backup_path)
                        backed_up_files += 1
            
            # Создаем метаданные бэкапа
            metadata = {
                'backup_date': timestamp,
                'files_count': backed_up_files,
                'project_version': self._get_project_version(),
                'data_sources': self.config.get('data_collectors', [])
            }
            
            with open(backup_path / 'backup_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Бэкап создан: {backed_up_files} файлов")
            
            # Сжимаем старые бэкапы если включено
            if self.config['backup_settings'].get('compress_old', True):
                self._compress_old_backups()
            
            # Удаляем старые бэкапы
            self._cleanup_old_backups()
            
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Ошибка создания бэкапа: {e}")
            return ""
    
    def _get_project_version(self) -> str:
        """Получает версию проекта (если есть git)"""
        try:
            result = subprocess.run(
                ['git', 'rev-parse', '--short', 'HEAD'],
                cwd=self.project_dir,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass
        return "unknown"
    
    def _compress_old_backups(self):
        """Сжимает старые бэкапы"""
        try:
            # Ищем бэкапы старше недели
            week_ago = datetime.now() - timedelta(weeks=1)
            
            for backup_dir in self.backup_dir.glob("etf_data_backup_*"):
                if backup_dir.is_dir():
                    # Извлекаем дату из имени
                    try:
                        date_str = backup_dir.name.split('_')[-2] + backup_dir.name.split('_')[-1]
                        backup_date = datetime.strptime(date_str, '%Y%m%d%H%M%S')
                        
                        if backup_date < week_ago:
                            # Сжимаем директорию
                            archive_path = f"{backup_dir}.tar.gz"
                            if not Path(archive_path).exists():
                                subprocess.run([
                                    'tar', '-czf', archive_path, '-C', 
                                    str(backup_dir.parent), backup_dir.name
                                ])
                                
                                # Удаляем исходную директорию
                                shutil.rmtree(backup_dir)
                                self.logger.info(f"Сжат бэкап: {backup_dir.name}")
                                
                    except ValueError:
                        continue
                        
        except Exception as e:
            self.logger.warning(f"Ошибка сжатия бэкапов: {e}")
    
    def _cleanup_old_backups(self):
        """Удаляет старые бэкапы согласно настройкам"""
        try:
            keep_count = self.config['backup_settings'].get('keep_backups', 12)
            
            # Получаем все бэкапы (и сжатые, и обычные)
            all_backups = []
            
            # Обычные директории
            for backup_dir in self.backup_dir.glob("etf_data_backup_*"):
                if backup_dir.is_dir():
                    all_backups.append(backup_dir)
            
            # Сжатые архивы
            for backup_archive in self.backup_dir.glob("etf_data_backup_*.tar.gz"):
                all_backups.append(backup_archive)
            
            # Сортируем по времени модификации
            all_backups.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # Удаляем лишние
            for old_backup in all_backups[keep_count:]:
                try:
                    if old_backup.is_dir():
                        shutil.rmtree(old_backup)
                    else:
                        old_backup.unlink()
                    self.logger.info(f"Удален старый бэкап: {old_backup.name}")
                except Exception as e:
                    self.logger.warning(f"Ошибка удаления {old_backup}: {e}")
                    
        except Exception as e:
            self.logger.warning(f"Ошибка очистки старых бэкапов: {e}")
    
    def update_data(self) -> bool:
        """Обновляет данные ETF"""
        
        self.logger.info("Начало обновления данных ETF")
        
        # Создаем бэкап перед обновлением
        backup_path = self.create_backup()
        if not backup_path:
            self.logger.error("Не удалось создать бэкап. Обновление отменено.")
            return False
        
        success = True
        
        try:
            # Запускаем сборщики данных
            for collector in self.config.get('data_collectors', []):
                collector_path = self.project_dir / collector
                
                if collector_path.exists():
                    self.logger.info(f"Запуск сборщика: {collector}")
                    
                    result = subprocess.run(
                        [sys.executable, str(collector_path)],
                        cwd=self.project_dir,
                        capture_output=True,
                        text=True,
                        timeout=1800  # 30 минут максимум
                    )
                    
                    if result.returncode == 0:
                        self.logger.info(f"Сборщик {collector} завершен успешно")
                    else:
                        self.logger.error(f"Ошибка в сборщике {collector}: {result.stderr}")
                        success = False
                else:
                    self.logger.warning(f"Сборщик не найден: {collector}")
            
            # Запускаем пост-обработку
            for script in self.config.get('post_update_scripts', []):
                script_path = self.project_dir / script
                
                if script_path.exists():
                    self.logger.info(f"Запуск пост-обработки: {script}")
                    
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        cwd=self.project_dir,
                        capture_output=True,
                        text=True,
                        timeout=600  # 10 минут максимум
                    )
                    
                    if result.returncode != 0:
                        self.logger.warning(f"Предупреждение в скрипте {script}: {result.stderr}")
            
            # Обновляем timestamp последнего обновления
            self._update_last_update_time()
            
            if success:
                self.logger.info("Обновление данных завершено успешно")
            else:
                self.logger.warning("Обновление завершено с ошибками")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка при обновлении: {e}")
            return False
    
    def _update_last_update_time(self):
        """Обновляет время последнего обновления"""
        try:
            update_info = {
                'last_update': datetime.now().isoformat(),
                'next_update': self._calculate_next_update().isoformat()
            }
            
            with open(self.project_dir / 'last_update.json', 'w', encoding='utf-8') as f:
                json.dump(update_info, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.warning(f"Ошибка записи времени обновления: {e}")
    
    def _calculate_next_update(self) -> datetime:
        """Вычисляет время следующего обновления"""
        now = datetime.now()
        
        # Находим следующее воскресенье в 02:00
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and now.hour >= 2:
            days_until_sunday = 7
        
        next_update = now + timedelta(days=days_until_sunday)
        next_update = next_update.replace(hour=2, minute=0, second=0, microsecond=0)
        
        return next_update
    
    def send_notification(self, success: bool, details: str = ""):
        """Отправляет уведомление о результате обновления"""
        
        if not self.config['notification'].get('enabled', False):
            return
        
        try:
            subject = f"{'✅' if success else '❌'} Обновление данных БПИФ - {datetime.now().strftime('%Y-%m-%d')}"
            
            body = f"""
Обновление данных БПИФ завершено.

Статус: {'Успешно' if success else 'С ошибками'}
Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{details}

---
Система автоматического обновления данных БПИФ
"""
            
            # Отправка email (если настроено)
            email_config = self.config['notification']
            if email_config.get('email'):
                self._send_email(
                    to_email=email_config['email'],
                    subject=subject,
                    body=body,
                    smtp_server=email_config.get('smtp_server'),
                    smtp_port=email_config.get('smtp_port', 587)
                )
            
        except Exception as e:
            self.logger.error(f"Ошибка отправки уведомления: {e}")
    
    def _send_email(self, to_email: str, subject: str, body: str, 
                   smtp_server: str, smtp_port: int):
        """Отправляет email уведомление"""
        
        try:
            msg = MimeMultipart()
            msg['Subject'] = subject
            msg['To'] = to_email
            
            msg.attach(MimeText(body, 'plain', 'utf-8'))
            
            # Здесь нужно настроить SMTP аутентификацию
            # Для безопасности используйте переменные окружения
            smtp_user = os.getenv('SMTP_USER')
            smtp_pass = os.getenv('SMTP_PASS')
            
            if smtp_user and smtp_pass:
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.send_message(msg)
                
                self.logger.info(f"Email уведомление отправлено на {to_email}")
            else:
                self.logger.warning("SMTP аутентификация не настроена")
                
        except Exception as e:
            self.logger.error(f"Ошибка отправки email: {e}")
    
    def run_update_job(self):
        """Задача обновления (вызывается по расписанию)"""
        self.logger.info("=== Запуск планового обновления ===")
        
        start_time = datetime.now()
        success = self.update_data()
        end_time = datetime.now()
        
        duration = end_time - start_time
        
        details = f"Время выполнения: {duration}"
        
        self.send_notification(success, details)
        
        self.logger.info(f"=== Плановое обновление завершено ({duration}) ===")
    
    def setup_schedule(self):
        """Настраивает расписание обновлений"""
        
        schedule_config = self.config['update_schedule']
        day = schedule_config.get('day_of_week', 'sunday')
        time_str = schedule_config.get('time', '02:00')
        
        self.logger.info(f"Настройка расписания: каждый {day} в {time_str}")
        
        # Настраиваем расписание
        getattr(schedule.every(), day).at(time_str).do(self.run_update_job)
        
        # Показываем время следующего обновления
        next_run = schedule.next_run()
        self.logger.info(f"Следующее обновление: {next_run}")
    
    def run_scheduler(self):
        """Запускает планировщик задач"""
        
        self.logger.info("Запуск планировщика автообновления...")
        self.setup_schedule()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
                
        except KeyboardInterrupt:
            self.logger.info("Планировщик остановлен пользователем")
        except Exception as e:
            self.logger.error(f"Ошибка планировщика: {e}")
    
    def run_manual_update(self):
        """Запускает ручное обновление"""
        
        self.logger.info("=== Запуск ручного обновления ===")
        
        success = self.update_data()
        
        if success:
            print("✅ Обновление завершено успешно")
        else:
            print("❌ Обновление завершено с ошибками")
        
        return success

def main():
    """Основная функция"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Система автоматического обновления данных БПИФ')
    parser.add_argument('--manual', action='store_true', help='Запустить ручное обновление')
    parser.add_argument('--schedule', action='store_true', help='Запустить планировщик')
    parser.add_argument('--backup', action='store_true', help='Создать только бэкап')
    parser.add_argument('--config', help='Путь к файлу конфигурации')
    
    args = parser.parse_args()
    
    # Создаем автообновлятор
    updater = AutoUpdater()
    
    if args.backup:
        print("📦 Создание бэкапа...")
        backup_path = updater.create_backup()
        if backup_path:
            print(f"✅ Бэкап создан: {backup_path}")
        else:
            print("❌ Ошибка создания бэкапа")
    
    elif args.manual:
        print("🔄 Запуск ручного обновления...")
        updater.run_manual_update()
    
    elif args.schedule:
        print("⏰ Запуск планировщика...")
        updater.run_scheduler()
    
    else:
        print("Система автоматического обновления данных БПИФ")
        print()
        print("Доступные команды:")
        print("  --manual     Запустить ручное обновление")
        print("  --schedule   Запустить планировщик (каждое воскресенье в 02:00)")
        print("  --backup     Создать бэкап данных")
        print()
        print("Конфигурация сохранена в: update_config.json")

if __name__ == "__main__":
    main()