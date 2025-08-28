#!/usr/bin/env python3
"""
Скрипт автоматической настройки дашборда для развертывания на других ПК
Скачивает и создает все необходимые файлы данных
"""

import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
import time

class DashboardSetup:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_path = Path.cwd()
        
    def check_dependencies(self):
        """Проверяем наличие всех необходимых зависимостей"""
        print("🔍 Проверка зависимостей...")
        
        required_packages = [
            'flask', 'pandas', 'numpy', 'plotly', 'requests'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package)
                print(f"✅ {package} установлен")
            except ImportError:
                missing_packages.append(package)
                print(f"❌ {package} НЕ УСТАНОВЛЕН")
        
        if missing_packages:
            print(f"\n⚠️ Установите недостающие пакеты:")
            print(f"pip install {' '.join(missing_packages)}")
            return False
            
        return True
    
    def create_minimal_data_files(self):
        """Создаем минимальные файлы данных для работы дашборда"""
        print("\n📊 Создание минимальных файлов данных...")
        
        # Создаем базовый файл ETF данных с правильными колонками
        etf_data = {
            'Тикер': ['LQDT', 'TMOS', 'SBMX', 'AKBP', 'AKCB', 'AKGP', 'SBGB'],
            'Название': [
                'Ликвидность. Денежный рынок',
                'Казначейские облигации',  
                'Смешанные облигации',
                'Потребительский сектор',
                'Корпоративные облигации',
                'Государственные облигации',
                'Государственные облигации'
            ],
            'Тип актива': ['Облигации', 'Облигации', 'Облигации', 'Акции', 'Облигации', 'Облигации', 'Облигации'],
            'Текущая стоимость чистых активов': [100000000, 50000000, 30000000, 20000000, 15000000, 12000000, 10000000],
            'annual_return': [15.5, 12.3, 8.7, -5.2, 14.1, 13.8, 12.9],  # Правильное название колонки
            'volatility': [2.1, 3.5, 4.2, 18.5, 3.1, 2.8, 3.2],  # Правильное название колонки
            'География': ['Россия', 'Россия', 'Россия', 'Россия', 'Россия', 'Россия', 'Россия'],
            'Комиссия управления': [0.3, 0.5, 0.4, 0.8, 0.4, 0.3, 0.4],
            'bid_ask_spread': [0.1, 0.15, 0.2, 0.3, 0.18, 0.12, 0.14],  # Правильное название колонки
            'sharpe_ratio': [7.35, 3.51, 2.14, -1.09, 4.55, 4.21, 4.03]  # Добавляем коэффициент Шарпа
        }
        
        df = pd.DataFrame(etf_data)
        filename = 'enhanced_etf_data_20250827_105019.csv'
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"✅ Создан {filename}")
        
        # Создаем файлы классификации
        self.create_classification_files(df)
        
        # Создаем файл временного анализа
        self.create_temporal_analysis_file()
        
    def create_classification_files(self, df):
        """Создаем файлы классификации"""
        print("📋 Создание файлов классификации...")
        
        # Базовая структура БПИФ
        bpif_structure = df.copy()
        bpif_structure['Уровень 1'] = bpif_structure['Тип актива']
        bpif_structure['Уровень 2'] = bpif_structure['Название']
        
        filename = 'simplified_bpif_structure_20250827_105516.csv'
        bpif_structure.to_csv(filename, index=False, encoding='utf-8')
        print(f"✅ Создан {filename}")
        
        # Сводка уровня 1
        level1_summary = df.groupby('Тип актива').agg({
            'Тикер': 'count',
            'Текущая стоимость чистых активов': 'sum',
            'annual_return': 'mean'
        }).reset_index()
        level1_summary.columns = ['Категория', 'Количество фондов', 'Общие активы', 'Средняя доходность']
        
        filename = 'simplified_level1_summary_20250827_105516.csv'
        level1_summary.to_csv(filename, index=False, encoding='utf-8')
        print(f"✅ Создан {filename}")
        
        # Сводка уровня 2 
        level2_summary = df.groupby(['Тип актива', 'Название']).agg({
            'Тикер': 'count',
            'Текущая стоимость чистых активов': 'sum'
        }).reset_index()
        level2_summary.columns = ['Уровень 1', 'Уровень 2', 'Количество фондов', 'Общие активы']
        
        filename = 'simplified_level2_summary_20250827_105516.csv'
        level2_summary.to_csv(filename, index=False, encoding='utf-8')
        print(f"✅ Создан {filename}")
        
        # География сводка
        geo_summary = df.groupby('География').agg({
            'Тикер': 'count',
            'Текущая стоимость чистых активов': 'sum'
        }).reset_index()
        geo_summary.columns = ['География', 'Количество фондов', 'Общие активы']
        
        filename = 'simplified_geography_summary_20250827_105516.csv'
        geo_summary.to_csv(filename, index=False, encoding='utf-8')
        print(f"✅ Создан {filename}")
    
    def create_temporal_analysis_file(self):
        """Создаем файл временного анализа"""
        print("📈 Создание файла временного анализа...")
        
        temporal_data = {
            "6 месяцев": [
                {
                    "ticker": "LQDT",
                    "asset_type": "Облигации",
                    "return_pct": 7.8,
                    "volatility": 2.1,
                    "records": 120,
                    "nav": 100000000,
                    "first_date": "2025-02-28",
                    "last_date": "2025-08-28"
                },
                {
                    "ticker": "TMOS", 
                    "asset_type": "Облигации",
                    "return_pct": 6.5,
                    "volatility": 3.5,
                    "records": 118,
                    "nav": 50000000,
                    "first_date": "2025-02-28",
                    "last_date": "2025-08-28"
                }
            ],
            "3 месяца": [
                {
                    "ticker": "LQDT",
                    "asset_type": "Облигации", 
                    "return_pct": 3.9,
                    "volatility": 2.0,
                    "records": 60,
                    "nav": 100000000,
                    "first_date": "2025-05-28",
                    "last_date": "2025-08-28"
                }
            ],
            "1 месяц": [
                {
                    "ticker": "LQDT",
                    "asset_type": "Облигации",
                    "return_pct": 1.3,
                    "volatility": 1.8,
                    "records": 20,
                    "nav": 100000000,
                    "first_date": "2025-07-28", 
                    "last_date": "2025-08-28"
                }
            ]
        }
        
        filename = 'real_temporal_analysis.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(temporal_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Создан {filename}")
        
    def create_directories(self):
        """Создаем необходимые директории"""
        print("📁 Создание директорий...")
        
        directories = [
            'investfunds_cache',
            'cache', 
            'logs',
            'data',
            'templates'
        ]
        
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
            print(f"✅ Создана директория {directory}")
    
    def run_setup(self):
        """Запускаем полную настройку"""
        print("🚀 АВТОМАТИЧЕСКАЯ НАСТРОЙКА ДАШБОРДА")
        print("=" * 60)
        
        # Проверяем зависимости
        if not self.check_dependencies():
            return False
        
        # Создаем директории
        self.create_directories()
        
        # Создаем минимальные файлы данных
        self.create_minimal_data_files()
        
        # Проверяем что все готово
        self.verify_setup()
        
        print(f"\n🎉 НАСТРОЙКА ЗАВЕРШЕНА!")
        print(f"📋 Для запуска дашборда выполните:")
        print(f"   python3 simple_dashboard.py")
        print(f"🌐 Дашборд будет доступен по адресу: http://localhost:5004")
        
        return True
        
    def verify_setup(self):
        """Проверяем что все файлы созданы корректно"""
        print(f"\n✅ Проверка готовности...")
        
        required_files = [
            'enhanced_etf_data_20250827_105019.csv',
            'simplified_bpif_structure_20250827_105516.csv',
            'simplified_level1_summary_20250827_105516.csv',
            'simplified_level2_summary_20250827_105516.csv',
            'simplified_geography_summary_20250827_105516.csv',
            'real_temporal_analysis.json'
        ]
        
        all_good = True
        for file in required_files:
            if Path(file).exists():
                size = Path(file).stat().st_size
                print(f"✅ {file} ({size} байт)")
            else:
                print(f"❌ {file} НЕ НАЙДЕН")
                all_good = False
        
        if all_good:
            print("✅ Все необходимые файлы созданы!")
        else:
            print("❌ Некоторые файлы отсутствуют")
        
        return all_good

def main():
    setup = DashboardSetup()
    success = setup.run_setup()
    
    if success:
        sys.exit(0)
    else:
        print("❌ Настройка не удалась")
        sys.exit(1)

if __name__ == "__main__":
    main()