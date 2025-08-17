#!/usr/bin/env python3
"""
Быстрый массовый маппер БПИФ на investfunds.ru
Загружает все фонды один раз, затем ищет локально
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import json
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path
import difflib
from dataclasses import dataclass

@dataclass
class FundInfo:
    """Информация о фонде с investfunds.ru"""
    fund_id: int
    name: str
    url: str
    type: str = ""

class BulkFundMapper:
    """Быстрый массовый маппер БПИФ"""
    
    def __init__(self):
        self.base_url = "https://investfunds.ru"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        self.logger = self._setup_logger()
        self.all_funds = []  # База всех фондов с сайта
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('BulkFundMapper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def load_all_funds(self) -> List[FundInfo]:
        """Загружает информацию о всех фондах с сайта"""
        
        self.logger.info("📥 Загружаем список всех фондов с investfunds.ru...")
        
        funds = []
        
        try:
            # Загружаем страницу со списком фондов
            funds_url = f"{self.base_url}/funds/"
            response = self.session.get(funds_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем все ссылки на фонды
            fund_links = soup.find_all('a', href=re.compile(r'/funds/\d+/'))
            
            self.logger.info(f"🔍 Найдено {len(fund_links)} ссылок на фонды")
            
            seen_ids = set()
            
            for link in fund_links:
                try:
                    href = link.get('href', '')
                    fund_id_match = re.search(r'/funds/(\d+)/', href)
                    
                    if not fund_id_match:
                        continue
                    
                    fund_id = int(fund_id_match.group(1))
                    
                    # Избегаем дубликатов
                    if fund_id in seen_ids:
                        continue
                    seen_ids.add(fund_id)
                    
                    fund_name = link.get_text(strip=True)
                    
                    if fund_name:
                        funds.append(FundInfo(
                            fund_id=fund_id,
                            name=fund_name,
                            url=f"{self.base_url}{href}"
                        ))
                
                except Exception as e:
                    self.logger.debug(f"Ошибка обработки ссылки: {e}")
                    continue
            
            self.logger.info(f"✅ Загружено {len(funds)} уникальных фондов")
            self.all_funds = funds
            
            return funds
            
        except Exception as e:
            self.logger.error(f"Ошибка загрузки списка фондов: {e}")
            return []
    
    def find_best_matches(self, ticker: str, fund_name: str, max_results: int = 5) -> List[Tuple[FundInfo, float]]:
        """Находит лучшие совпадения среди загруженных фондов"""
        
        matches = []
        
        # Очищаем название для поиска
        clean_search_name = self._clean_fund_name(fund_name)
        
        for fund in self.all_funds:
            # Несколько стратегий поиска
            
            # 1. Прямое вхождение тикера в название
            ticker_score = 0
            if ticker.upper() in fund.name.upper():
                ticker_score = 0.9
                
            # 2. Схожесть очищенных названий
            clean_fund_name = self._clean_fund_name(fund.name)
            name_similarity = difflib.SequenceMatcher(None, clean_search_name.lower(), clean_fund_name.lower()).ratio()
            
            # 3. Совпадение ключевых слов
            search_keywords = set(re.findall(r'\b\w{3,}\b', clean_search_name.lower()))
            fund_keywords = set(re.findall(r'\b\w{3,}\b', clean_fund_name.lower()))
            
            keyword_score = 0
            if search_keywords and fund_keywords:
                keyword_score = len(search_keywords & fund_keywords) / len(search_keywords | fund_keywords)
            
            # Итоговый скор
            final_score = max(ticker_score, name_similarity, keyword_score * 0.8)
            
            if final_score > 0.3:  # Минимальный порог
                matches.append((fund, final_score))
        
        # Сортируем по скору
        matches.sort(key=lambda x: x[1], reverse=True)
        
        return matches[:max_results]
    
    def _clean_fund_name(self, name: str) -> str:
        """Очищает название фонда для поиска"""
        
        clean = name
        
        # Убираем общие слова и аббревиатуры
        patterns_to_remove = [
            r'\b(БПИФ|ETF|Фонд|УК|Управление)\b',
            r'[«»"\'()].*?[«»"\'()]',
            r'\([^)]*\)',
            r'–\s*\d+',  # Убираем номера после тире
        ]
        
        for pattern in patterns_to_remove:
            clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)
        
        # Убираем лишние пробелы
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        return clean
    
    def auto_map_funds(self, etf_data: pd.DataFrame, confidence_threshold: float = 0.8) -> Dict[str, int]:
        """Автоматически маппит фонды с высокой уверенностью"""
        
        if not self.all_funds:
            self.load_all_funds()
        
        mapping = {}
        stats = {
            'high_confidence': 0,
            'medium_confidence': 0,
            'low_confidence': 0,
            'no_matches': 0
        }
        
        self.logger.info(f"🤖 Автоматический маппинг {len(etf_data)} фондов...")
        self.logger.info(f"📊 База данных: {len(self.all_funds)} фондов с investfunds.ru")
        
        results = []
        
        for idx, row in etf_data.iterrows():
            ticker = row['ticker']
            fund_name = row.get('full_name', row.get('short_name', ticker))
            
            matches = self.find_best_matches(ticker, fund_name)
            
            if matches:
                best_match, confidence = matches[0]
                
                result = {
                    'ticker': ticker,
                    'search_name': fund_name,
                    'best_match_id': best_match.fund_id,
                    'best_match_name': best_match.name,
                    'confidence': confidence,
                    'alternatives': [(m[0].fund_id, m[0].name, m[1]) for m in matches[1:3]]
                }
                
                if confidence >= confidence_threshold:
                    mapping[ticker] = best_match.fund_id
                    stats['high_confidence'] += 1
                    result['status'] = 'auto_mapped'
                    self.logger.info(f"✅ {ticker}: {best_match.fund_id} (уверенность: {confidence:.2f})")
                elif confidence >= 0.6:
                    stats['medium_confidence'] += 1
                    result['status'] = 'needs_review'
                    self.logger.info(f"⚠️  {ticker}: {best_match.fund_id} (требует проверки: {confidence:.2f})")
                else:
                    stats['low_confidence'] += 1
                    result['status'] = 'low_confidence'
                    self.logger.info(f"❓ {ticker}: {best_match.fund_id} (низкая уверенность: {confidence:.2f})")
            else:
                stats['no_matches'] += 1
                result = {
                    'ticker': ticker,
                    'search_name': fund_name,
                    'status': 'no_matches'
                }
                self.logger.warning(f"❌ {ticker}: совпадений не найдено")
            
            results.append(result)
        
        # Сохраняем детальные результаты
        results_df = pd.DataFrame(results)
        results_df.to_csv('fund_mapping_results.csv', index=False, encoding='utf-8')
        
        # Сохраняем итоговый маппинг
        mapping_data = {
            'mapping': mapping,
            'stats': stats,
            'confidence_threshold': confidence_threshold,
            'total_funds_analyzed': len(etf_data),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open('auto_fund_mapping.json', 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)
        
        # Выводим статистику
        self.logger.info(f"\n📊 Результаты автоматического маппинга:")
        self.logger.info(f"   ✅ Высокая уверенность: {stats['high_confidence']}")
        self.logger.info(f"   ⚠️  Требует проверки: {stats['medium_confidence']}")
        self.logger.info(f"   ❓ Низкая уверенность: {stats['low_confidence']}")
        self.logger.info(f"   ❌ Не найдено: {stats['no_matches']}")
        self.logger.info(f"   📈 Процент автомаппинга: {stats['high_confidence']/len(etf_data)*100:.1f}%")
        
        return mapping
    
    def generate_review_report(self) -> str:
        """Генерирует отчет для ручной проверки"""
        
        try:
            results_df = pd.read_csv('fund_mapping_results.csv')
        except:
            self.logger.error("Файл результатов не найден")
            return ""
        
        report = []
        report.append("# 📋 Отчет по маппингу БПИФ на investfunds.ru\n")
        
        # Фонды, требующие проверки
        needs_review = results_df[results_df['status'] == 'needs_review']
        if not needs_review.empty:
            report.append("## ⚠️ Требуют ручной проверки:\n")
            for _, row in needs_review.iterrows():
                report.append(f"### {row['ticker']} - {row['search_name']}")
                report.append(f"**Предлагаемый ID:** {row['best_match_id']}")
                report.append(f"**Название на сайте:** {row['best_match_name']}")
                report.append(f"**Уверенность:** {row['confidence']:.2f}")
                report.append(f"**URL:** https://investfunds.ru/funds/{row['best_match_id']}/")
                report.append("")
        
        # Фонды с низкой уверенностью
        low_confidence = results_df[results_df['status'] == 'low_confidence']
        if not low_confidence.empty:
            report.append("## ❓ Низкая уверенность:\n")
            for _, row in low_confidence.iterrows():
                report.append(f"- **{row['ticker']}**: {row['best_match_name']} (ID: {row['best_match_id']}, уверенность: {row['confidence']:.2f})")
        
        # Не найденные фонды
        no_matches = results_df[results_df['status'] == 'no_matches']
        if not no_matches.empty:
            report.append("## ❌ Не найдены совпадения:\n")
            for _, row in no_matches.iterrows():
                report.append(f"- **{row['ticker']}**: {row['search_name']}")
        
        report_text = "\n".join(report)
        
        with open('fund_mapping_review.md', 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        return report_text

def main():
    """Основная функция"""
    
    # Загружаем данные ETF
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if not data_files:
        data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
    
    if not data_files:
        print("❌ Файлы с данными ETF не найдены")
        return
    
    data_file = max(data_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 Загружаем данные из {data_file}")
    
    etf_data = pd.read_csv(data_file)
    print(f"✅ Загружено {len(etf_data)} ETF")
    
    # Создаем маппер
    mapper = BulkFundMapper()
    
    # Загружаем все фонды
    mapper.load_all_funds()
    
    # Автоматический маппинг
    mapping = mapper.auto_map_funds(etf_data, confidence_threshold=0.8)
    
    # Генерируем отчет для проверки
    mapper.generate_review_report()
    
    print(f"\n🎉 Маппинг завершен!")
    print(f"📄 Файлы созданы:")
    print(f"   - auto_fund_mapping.json (итоговый маппинг)")
    print(f"   - fund_mapping_results.csv (детальные результаты)")
    print(f"   - fund_mapping_review.md (отчет для проверки)")

if __name__ == "__main__":
    main()