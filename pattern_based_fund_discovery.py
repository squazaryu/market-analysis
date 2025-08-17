#!/usr/bin/env python3
"""
Обнаружение фондов на основе известных паттернов и прямой проверки ID
Более эффективный подход для investfunds.ru
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
import time
import json
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

class PatternBasedFundDiscovery:
    """Обнаружение фондов на основе паттернов"""
    
    def __init__(self, max_workers: int = 3):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://investfunds.ru"
        self.max_workers = max_workers
        self.logger = self._setup_logger()
        
        # Известные ID и паттерны
        self.known_mappings = {
            'LQDT': 5973,
            'AKMB': 6225,
            'AKGD': 10147,
        }
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('PatternFundDiscovery')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def check_fund_id_for_ticker(self, fund_id: int, ticker: str) -> Optional[Dict]:
        """Проверяет, содержит ли страница фонда указанный тикер"""
        
        try:
            url = f"{self.base_url}/funds/{fund_id}/"
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text().upper()
            
            # Различные варианты поиска тикера
            ticker_patterns = [
                ticker.upper(),
                f"БПИФ {ticker.upper()}",
                f"ETF {ticker.upper()}",
                f"{ticker.upper()} ",
                f" {ticker.upper()}",
                f"({ticker.upper()})",
                f"[{ticker.upper()}]",
            ]
            
            ticker_found = any(pattern in page_text for pattern in ticker_patterns)
            
            if ticker_found:
                # Извлекаем данные
                title = soup.find('h1')
                fund_name = title.get_text(strip=True) if title else 'Unknown'
                
                # Ищем СЧА
                nav_value = self._extract_nav_from_page(page_text)
                unit_price = self._extract_unit_price_from_page(page_text)
                
                return {
                    'fund_id': fund_id,
                    'ticker': ticker,
                    'name': fund_name,
                    'nav': nav_value or 0,
                    'unit_price': unit_price or 0,
                    'url': url,
                    'page_contains_ticker': True
                }
            
            return None
            
        except Exception as e:
            if fund_id % 1000 == 0:  # Логируем только каждую 1000 ошибку
                self.logger.warning(f"Ошибка проверки ID {fund_id}: {e}")
            return None
    
    def _extract_nav_from_page(self, page_text: str) -> Optional[float]:
        """Извлекает СЧА со страницы"""
        
        # Паттерны для поиска СЧА
        nav_patterns = [
            r'СЧА[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'чистых активов[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'активы[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'(\d{1,3}(?:\s\d{3})*(?:\s\d{3})*(?:\.\d+)?)',  # Большие числа с пробелами
        ]
        
        for pattern in nav_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                try:
                    # Очищаем число
                    cleaned = re.sub(r'[^\d.]', '', match.replace(' ', '').replace(',', ''))
                    if cleaned:
                        value = float(cleaned)
                        # СЧА должна быть больше 10 млн рублей
                        if value > 10_000_000:
                            return value
                except ValueError:
                    continue
        
        return None
    
    def _extract_unit_price_from_page(self, page_text: str) -> Optional[float]:
        """Извлекает цену пая со страницы"""
        
        # Паттерны для поиска цены пая
        price_patterns = [
            r'цена пая[:\s]*([0-9.,]+)',
            r'стоимость пая[:\s]*([0-9.,]+)',
            r'пай[:\s]*([0-9.,]+)',
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                try:
                    cleaned = re.sub(r'[^\d.]', '', match.replace(',', '.'))
                    if cleaned:
                        value = float(cleaned)
                        # Цена пая обычно от 0.1 до 10000 рублей
                        if 0.1 <= value <= 10000:
                            return value
                except ValueError:
                    continue
        
        return None
    
    def scan_id_ranges_for_tickers(self, tickers: List[str], id_ranges: List[Tuple[int, int]]) -> Dict[str, Dict]:
        """Сканирует диапазоны ID для поиска тикеров"""
        
        results = {}
        total_checks = sum(end - start + 1 for start, end in id_ranges) * len(tickers)
        
        self.logger.info(f"🔍 Сканируем {len(tickers)} тикеров в {len(id_ranges)} диапазонах")
        self.logger.info(f"📊 Общее количество проверок: {total_checks}")
        
        for ticker in tickers:
            if ticker in results:
                continue
                
            self.logger.info(f"\n🎯 Ищем {ticker}")
            
            found = False
            for start_id, end_id in id_ranges:
                if found:
                    break
                    
                self.logger.info(f"   Диапазон {start_id}-{end_id}")
                
                # Проверяем диапазон с многопоточностью
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(self.check_fund_id_for_ticker, fund_id, ticker): fund_id
                        for fund_id in range(start_id, end_id + 1)
                    }
                    
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            results[ticker] = result
                            found = True
                            self.logger.info(f"✅ {ticker} -> ID {result['fund_id']} (СЧА: {result['nav']/1e9:.1f} млрд ₽)")
                            break
            
            if not found:
                self.logger.warning(f"❌ {ticker}: не найден")
        
        return results
    
    def intelligent_search(self, etf_data: pd.DataFrame, max_funds: int = 50) -> Dict[str, Dict]:
        """Умный поиск с использованием известных паттернов"""
        
        # Начинаем с известных маппингов
        results = {}
        
        # Добавляем уже известные фонды
        self.logger.info("📋 Проверяем известные маппинги")
        for ticker, fund_id in self.known_mappings.items():
            if ticker in etf_data['ticker'].values:
                try:
                    from investfunds_parser import InvestFundsParser
                    parser = InvestFundsParser()
                    fund_data = parser.get_fund_data(fund_id)
                    
                    if fund_data and fund_data.get('nav', 0) > 0:
                        results[ticker] = {
                            'fund_id': fund_id,
                            'ticker': ticker,
                            'nav': fund_data['nav'],
                            'unit_price': fund_data['unit_price'],
                            'name': fund_data['name'],
                            'verified': True
                        }
                        self.logger.info(f"✅ {ticker} (известный): СЧА {fund_data['nav']/1e9:.1f} млрд ₽")
                except Exception as e:
                    self.logger.warning(f"⚠️ Ошибка проверки известного {ticker}: {e}")
        
        # Ищем остальные фонды
        remaining_tickers = [
            ticker for ticker in etf_data['ticker'].tolist()[:max_funds]
            if ticker not in results
        ]
        
        if not remaining_tickers:
            return results
        
        self.logger.info(f"🔍 Ищем {len(remaining_tickers)} новых фондов")
        
        # Определяем перспективные диапазоны на основе известных ID
        known_ids = list(self.known_mappings.values())
        if known_ids:
            min_known = min(known_ids)
            max_known = max(known_ids)
            
            # Умные диапазоны вокруг известных ID
            search_ranges = [
                (max_known + 1, min(max_known + 1000, 15000)),    # После максимального
                (max(1, min_known - 1000), min_known - 1),        # До минимального
                (min_known + 1, max_known - 1),                   # Между известными
                (1, 3000),                                        # Ранние фонды
                (8000, 12000),                                    # Средний диапазон
            ]
        else:
            # Стандартные диапазоны
            search_ranges = [
                (1, 3000),
                (5000, 8000),
                (10000, 13000),
            ]
        
        # Поиск в приоритетных диапазонах
        new_results = self.scan_id_ranges_for_tickers(remaining_tickers, search_ranges)
        results.update(new_results)
        
        return results
    
    def create_expanded_mapping(self, results: Dict[str, Dict]) -> Dict[str, int]:
        """Создает расширенный маппинг для investfunds_parser.py"""
        
        mapping = {}
        for ticker, data in results.items():
            if 'fund_id' in data and data.get('nav', 0) > 0:
                mapping[ticker] = data['fund_id']
        
        return mapping
    
    def update_investfunds_parser(self, new_mappings: Dict[str, int]):
        """Обновляет маппинг в investfunds_parser.py"""
        
        try:
            parser_file = Path("investfunds_parser.py")
            if not parser_file.exists():
                self.logger.error("❌ Файл investfunds_parser.py не найден")
                return
            
            # Читаем файл
            with open(parser_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Ищем маппинг
            mapping_start = content.find("self.fund_mapping = {")
            mapping_end = content.find("}", mapping_start) + 1
            
            if mapping_start == -1:
                self.logger.error("❌ Не найден fund_mapping в файле")
                return
            
            # Создаем новый маппинг
            mapping_lines = ["        # Расширенный маппинг тикеров на ID фондов"]
            mapping_lines.append("        self.fund_mapping = {")
            
            for ticker, fund_id in sorted(new_mappings.items()):
                mapping_lines.append(f"            '{ticker}': {fund_id},  # Автоматически найден")
            
            mapping_lines.append("        }")
            
            # Создаем бэкап
            backup_file = f"investfunds_parser_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.py"
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Записываем обновленный файл
            new_content = (
                content[:mapping_start] + 
                "\n".join(mapping_lines) + 
                content[mapping_end:]
            )
            
            with open(parser_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            self.logger.info(f"✅ Обновлен маппинг: {len(new_mappings)} фондов")
            self.logger.info(f"📄 Бэкап: {backup_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления: {e}")

def main():
    """Основная функция"""
    
    # Загружаем данные
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if not data_files:
        data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
    
    if not data_files:
        print("❌ Файлы с данными ETF не найдены")
        return
    
    latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
    etf_data = pd.read_csv(latest_data)
    
    print(f"📊 Загружено {len(etf_data)} ETF")
    
    # Создаем поисковик
    discoverer = PatternBasedFundDiscovery(max_workers=3)
    
    print(f"\n🚀 Запускаем умный поиск (первые 30 фондов)")
    
    # Умный поиск
    results = discoverer.intelligent_search(etf_data, max_funds=30)
    
    # Создаем расширенный маппинг
    new_mappings = discoverer.create_expanded_mapping(results)
    
    # Сохраняем результаты
    save_data = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'total_found': len(results),
        'mappings': new_mappings,
        'detailed_results': {k: v for k, v in results.items() if 'nav' in v}
    }
    
    with open('pattern_discovery_results.json', 'w', encoding='utf-8') as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n🎉 РЕЗУЛЬТАТЫ УМНОГО ПОИСКА:")
    print(f"   ✅ Найдено: {len(results)} фондов")
    print(f"   📈 Покрытие: {len(results)/30*100:.1f}%")
    
    if new_mappings:
        print(f"\n🏆 Найденные маппинги:")
        for ticker, fund_id in sorted(new_mappings.items()):
            nav = results[ticker].get('nav', 0)
            print(f"   {ticker} -> ID {fund_id} (СЧА: {nav/1e9:.1f} млрд ₽)")
        
        # Обновляем parser
        discoverer.update_investfunds_parser(new_mappings)
    
    print(f"\n💾 Результаты сохранены в pattern_discovery_results.json")

if __name__ == "__main__":
    main()