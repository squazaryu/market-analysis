#!/usr/bin/env python3
"""
Полный сборщик данных для всех ETF на MOEX
Автоматически обнаруживает все ETF и собирает по ним данные
"""

import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Импортируем нашу существующую систему
from fallback_manager import DataProviderManager
from etf_data_collector import ETFDataCollectorWithFallback

class FullMOEXETFCollector:
    """Полный сборщик данных для всех ETF на MOEX"""
    
    def __init__(self):
        self.fallback_manager = DataProviderManager()
        self.etf_collector = ETFDataCollectorWithFallback()
        self.logger = self._setup_logging()
        
        # Кэш для списка ETF
        self.etf_cache = {}
        self.cache_timestamp = None
        self.cache_ttl = 3600  # 1 час
    
    def _setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def get_all_moex_etf(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Получает список всех ETF с MOEX
        
        Args:
            force_refresh: Принудительно обновить кэш
            
        Returns:
            DataFrame со всеми ETF
        """
        
        # Проверяем кэш
        if not force_refresh and self._is_cache_valid():
            self.logger.info("📊 Используем кэшированный список ETF")
            return pd.DataFrame(self.etf_cache)
        
        try:
            self.logger.info("📊 Получаем актуальный список ETF с MOEX...")
            
            # Получаем ETF с основной доски
            url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQTF/securities.json"
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            securities = data['securities']['data']
            columns = data['securities']['columns']
            
            # Создаем DataFrame
            etf_df = pd.DataFrame(securities, columns=columns)
            
            # Получаем дополнительную информацию о каждом ETF
            self.logger.info("📋 Получаем детальную информацию о каждом ETF...")
            
            enhanced_etf_data = []
            
            for _, etf in etf_df.iterrows():
                ticker = etf['SECID']
                
                try:
                    # Получаем детальную информацию
                    detail_info = self._get_etf_details(ticker)
                    
                    etf_info = {
                        'ticker': ticker,
                        'short_name': etf.get('SHORTNAME', ''),
                        'full_name': etf.get('SECNAME', ''),
                        'isin': etf.get('ISIN', ''),
                        'reg_number': etf.get('REGNUMBER', ''),
                        'lot_size': etf.get('LOTSIZE', 1),
                        'face_value': etf.get('FACEVALUE', 0),
                        'status': etf.get('STATUS', ''),
                        'board_id': etf.get('BOARDID', ''),
                        'market_code': etf.get('MARKETCODE', ''),
                        'engine_id': etf.get('ENGINE', ''),
                        'type': etf.get('TYPE', ''),
                        'group': etf.get('GROUP', ''),
                        'is_traded': etf.get('STATUS') == 'A',  # A = Active
                        **detail_info
                    }
                    
                    enhanced_etf_data.append(etf_info)
                    
                    # Небольшая пауза между запросами
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Не удалось получить детали для {ticker}: {e}")
                    
                    # Добавляем базовую информацию
                    etf_info = {
                        'ticker': ticker,
                        'short_name': etf.get('SHORTNAME', ''),
                        'full_name': etf.get('SECNAME', ''),
                        'isin': etf.get('ISIN', ''),
                        'reg_number': etf.get('REGNUMBER', ''),
                        'lot_size': etf.get('LOTSIZE', 1),
                        'face_value': etf.get('FACEVALUE', 0),
                        'status': etf.get('STATUS', ''),
                        'board_id': etf.get('BOARDID', ''),
                        'market_code': etf.get('MARKETCODE', ''),
                        'engine_id': etf.get('ENGINE', ''),
                        'type': etf.get('TYPE', ''),
                        'group': etf.get('GROUP', ''),
                        'is_traded': etf.get('STATUS') == 'A',
                        'management_company': '',
                        'expense_ratio': None,
                        'inception_date': None,
                        'nav': None,
                        'assets_under_management': None
                    }
                    
                    enhanced_etf_data.append(etf_info)
            
            # Создаем итоговый DataFrame
            result_df = pd.DataFrame(enhanced_etf_data)
            
            # Обновляем кэш
            self.etf_cache = result_df.to_dict('records')
            self.cache_timestamp = datetime.now()
            
            self.logger.info(f"✅ Получено {len(result_df)} ETF с MOEX")
            self.logger.info(f"   • Активных: {result_df['is_traded'].sum()}")
            self.logger.info(f"   • Неактивных: {(~result_df['is_traded']).sum()}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения списка ETF: {e}")
            
            # Возвращаем кэшированные данные если есть
            if self.etf_cache:
                self.logger.info("📊 Используем устаревшие кэшированные данные")
                return pd.DataFrame(self.etf_cache)
            
            raise
    
    def _get_etf_details(self, ticker: str) -> Dict:
        """Получает детальную информацию об ETF"""
        
        try:
            # Получаем информацию о фонде
            url = f"https://iss.moex.com/iss/securities/{ticker}.json"
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Извлекаем полезную информацию
            details = {}
            
            # Ищем информацию в разных секциях
            if 'description' in data and data['description']['data']:
                desc_data = data['description']['data']
                desc_columns = data['description']['columns']
                
                for row in desc_data:
                    if len(row) >= 3:
                        name = row[0] if len(row) > 0 else ''
                        value = row[2] if len(row) > 2 else ''
                        
                        if name == 'TYPENAME':
                            details['fund_type'] = value
                        elif name == 'EMITTER_TITLE':
                            details['management_company'] = value
                        elif name == 'LISTLEVEL':
                            details['listing_level'] = value
                        elif name == 'ISSUEDATE':
                            details['inception_date'] = value
            
            return details
            
        except Exception as e:
            self.logger.debug(f"Не удалось получить детали для {ticker}: {e}")
            return {
                'management_company': '',
                'expense_ratio': None,
                'inception_date': None,
                'nav': None,
                'assets_under_management': None
            }
    
    def _is_cache_valid(self) -> bool:
        """Проверяет валидность кэша"""
        if not self.etf_cache or not self.cache_timestamp:
            return False
        
        return (datetime.now() - self.cache_timestamp).seconds < self.cache_ttl
    
    def collect_all_etf_data(self, active_only: bool = True) -> pd.DataFrame:
        """
        Собирает данные по всем ETF
        
        Args:
            active_only: Собирать данные только по активным ETF
            
        Returns:
            DataFrame с данными по всем ETF
        """
        
        # Получаем список ETF
        etf_list = self.get_all_moex_etf()
        
        if active_only:
            etf_list = etf_list[etf_list['is_traded'] == True]
            self.logger.info(f"📊 Собираем данные по {len(etf_list)} активным ETF")
        else:
            self.logger.info(f"📊 Собираем данные по всем {len(etf_list)} ETF")
        
        # Собираем данные по каждому ETF
        all_etf_data = []
        successful_collections = 0
        failed_collections = 0
        
        for idx, etf_info in etf_list.iterrows():
            ticker = etf_info['ticker']
            
            try:
                self.logger.info(f"📈 Собираем данные для {ticker} ({idx + 1}/{len(etf_list)})")
                
                # Используем существующий коллектор
                etf_data = self.etf_collector.collect_etf_data(ticker)
                
                if etf_data:
                    # Добавляем метаданные из списка ETF
                    etf_data.update({
                        'short_name': etf_info['short_name'],
                        'full_name': etf_info['full_name'],
                        'isin': etf_info['isin'],
                        'reg_number': etf_info['reg_number'],
                        'management_company': etf_info.get('management_company', ''),
                        'is_traded': etf_info['is_traded'],
                        'listing_level': etf_info.get('listing_level', ''),
                        'inception_date': etf_info.get('inception_date', ''),
                        'collection_timestamp': datetime.now().isoformat()
                    })
                    
                    all_etf_data.append(etf_data)
                    successful_collections += 1
                else:
                    self.logger.warning(f"⚠️ Не удалось собрать данные для {ticker}")
                    failed_collections += 1
                
                # Пауза между запросами
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка сбора данных для {ticker}: {e}")
                failed_collections += 1
                continue
        
        # Создаем итоговый DataFrame
        if all_etf_data:
            result_df = pd.DataFrame(all_etf_data)
            
            self.logger.info(f"✅ Сбор данных завершен:")
            self.logger.info(f"   • Успешно: {successful_collections}")
            self.logger.info(f"   • Ошибок: {failed_collections}")
            self.logger.info(f"   • Успешность: {successful_collections/(successful_collections+failed_collections)*100:.1f}%")
            
            return result_df
        else:
            self.logger.error("❌ Не удалось собрать данные ни по одному ETF")
            return pd.DataFrame()
    
    def create_comprehensive_report(self, etf_data: pd.DataFrame) -> Dict:
        """Создает комплексный отчет по всем ETF"""
        
        if etf_data.empty:
            return {}
        
        self.logger.info("📊 Создаем комплексный отчет...")
        
        report = {
            'summary': {
                'total_etf': len(etf_data),
                'active_etf': etf_data['is_traded'].sum() if 'is_traded' in etf_data.columns else len(etf_data),
                'total_market_cap': etf_data['market_cap'].sum() if 'market_cap' in etf_data.columns else 0,
                'average_return_1y': etf_data['annual_return'].mean() if 'annual_return' in etf_data.columns else 0,
                'average_volatility': etf_data['volatility'].mean() if 'volatility' in etf_data.columns else 0,
                'report_date': datetime.now().isoformat()
            },
            'top_performers': {},
            'management_companies': {},
            'categories': {},
            'risk_analysis': {}
        }
        
        # Топ исполнители
        if 'annual_return' in etf_data.columns:
            top_return = etf_data.nlargest(10, 'annual_return')[['ticker', 'short_name', 'annual_return']].to_dict('records')
            report['top_performers']['by_return'] = top_return
        
        if 'avg_daily_volume' in etf_data.columns:
            top_volume = etf_data.nlargest(10, 'avg_daily_volume')[['ticker', 'short_name', 'avg_daily_volume']].to_dict('records')
            report['top_performers']['by_volume'] = top_volume
        
        # Анализ по управляющим компаниям
        if 'management_company' in etf_data.columns:
            mc_stats = etf_data.groupby('management_company').agg({
                'ticker': 'count',
                'market_cap': 'sum' if 'market_cap' in etf_data.columns else lambda x: 0,
                'annual_return': 'mean' if 'annual_return' in etf_data.columns else lambda x: 0
            }).round(2)
            
            report['management_companies'] = mc_stats.to_dict('index')
        
        # Анализ рисков
        if 'volatility' in etf_data.columns and 'annual_return' in etf_data.columns:
            # Разделяем на категории риска
            low_risk = etf_data[etf_data['volatility'] < etf_data['volatility'].quantile(0.33)]
            medium_risk = etf_data[(etf_data['volatility'] >= etf_data['volatility'].quantile(0.33)) & 
                                 (etf_data['volatility'] < etf_data['volatility'].quantile(0.67))]
            high_risk = etf_data[etf_data['volatility'] >= etf_data['volatility'].quantile(0.67)]
            
            report['risk_analysis'] = {
                'low_risk': {
                    'count': len(low_risk),
                    'avg_return': low_risk['annual_return'].mean(),
                    'avg_volatility': low_risk['volatility'].mean()
                },
                'medium_risk': {
                    'count': len(medium_risk),
                    'avg_return': medium_risk['annual_return'].mean(),
                    'avg_volatility': medium_risk['volatility'].mean()
                },
                'high_risk': {
                    'count': len(high_risk),
                    'avg_return': high_risk['annual_return'].mean(),
                    'avg_volatility': high_risk['volatility'].mean()
                }
            }
        
        return report
    
    def save_results(self, etf_data: pd.DataFrame, report: Dict, prefix: str = "full_moex_etf"):
        """Сохраняет результаты анализа"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем данные
        if not etf_data.empty:
            csv_file = f"{prefix}_data_{timestamp}.csv"
            etf_data.to_csv(csv_file, index=False, encoding='utf-8')
            self.logger.info(f"💾 Данные ETF сохранены в {csv_file}")
        
        # Сохраняем отчет
        if report:
            json_file = f"{prefix}_report_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                # Конвертируем numpy типы в обычные Python типы
                json.dump(report, f, ensure_ascii=False, indent=2, default=self._json_serializer)
            self.logger.info(f"💾 Отчет сохранен в {json_file}")
        
        return csv_file if not etf_data.empty else None, json_file if report else None
    
    def _json_serializer(self, obj):
        """Сериализатор для JSON, обрабатывающий numpy типы"""
        import numpy as np
        
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def main():
    """Основная функция для запуска полного анализа"""
    
    collector = FullMOEXETFCollector()
    
    try:
        # Получаем список всех ETF
        print("🚀 Запускаем полный анализ ETF на MOEX...")
        etf_list = collector.get_all_moex_etf()
        
        print(f"\n📊 Найдено ETF на MOEX:")
        print(f"   • Всего: {len(etf_list)}")
        print(f"   • Активных: {etf_list['is_traded'].sum()}")
        print(f"   • Неактивных: {(~etf_list['is_traded']).sum()}")
        
        # Собираем данные по всем активным ETF
        etf_data = collector.collect_all_etf_data(active_only=True)
        
        if not etf_data.empty:
            # Создаем отчет
            report = collector.create_comprehensive_report(etf_data)
            
            # Сохраняем результаты
            data_file, report_file = collector.save_results(etf_data, report)
            
            print(f"\n✅ Анализ завершен успешно!")
            print(f"📊 Проанализировано {len(etf_data)} ETF")
            print(f"💾 Результаты сохранены:")
            if data_file:
                print(f"   • Данные: {data_file}")
            if report_file:
                print(f"   • Отчет: {report_file}")
            
            # Показываем краткую статистику
            if report and 'summary' in report:
                summary = report['summary']
                print(f"\n📈 Краткая статистика:")
                print(f"   • Средняя доходность: {summary.get('average_return_1y', 0):.2f}%")
                print(f"   • Средняя волатильность: {summary.get('average_volatility', 0):.2f}%")
                
                if 'top_performers' in report and 'by_return' in report['top_performers']:
                    top_etf = report['top_performers']['by_return'][0]
                    print(f"   • Лучший ETF: {top_etf['ticker']} ({top_etf['annual_return']:.2f}%)")
        
        else:
            print("❌ Не удалось собрать данные по ETF")
    
    except Exception as e:
        print(f"❌ Ошибка выполнения анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()