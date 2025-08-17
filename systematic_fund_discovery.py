#!/usr/bin/env python3
"""
Систематическое обнаружение фондов на investfunds.ru
Более эффективный подход к поиску реальных соответствий
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
import json
from typing import Dict, List, Optional, Set
from pathlib import Path
import concurrent.futures
from threading import Lock

class SystematicFundDiscovery:
    """Систематическое обнаружение фондов"""
    
    def __init__(self, max_workers: int = 5):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://investfunds.ru"
        self.max_workers = max_workers
        self.found_mappings = {}
        self.lock = Lock()
        
    def extract_ticker_from_page(self, soup: BeautifulSoup, page_text: str) -> Optional[str]:
        """Извлекает тикер со страницы фонда"""
        
        # Паттерны для поиска тикера
        ticker_patterns = [
            r'\b([A-Z]{4,6})\b',  # 4-6 заглавных букв
            r'тикер[:\s]*([A-Z]+)',
            r'символ[:\s]*([A-Z]+)',
            r'код[:\s]*([A-Z]+)',
        ]
        
        potential_tickers = set()
        
        for pattern in ticker_patterns:
            matches = re.findall(pattern, page_text.upper())
            for match in matches:
                if 3 <= len(match) <= 6:  # Разумная длина тикера
                    potential_tickers.add(match)
        
        # Дополнительная проверка в заголовке
        title = soup.find('h1')
        if title:
            title_text = title.get_text().upper()
            # Ищем в скобках или после запятой
            bracket_match = re.search(r'[(),]\s*([A-Z]{3,6})', title_text)
            if bracket_match:
                potential_tickers.add(bracket_match.group(1))
        
        # Фильтруем очевидно неправильные тикеры
        exclude_words = {'RUB', 'USD', 'EUR', 'HTML', 'HTTP', 'HTTPS', 'WWW', 'COM', 'ORG'}
        valid_tickers = [t for t in potential_tickers if t not in exclude_words]
        
        return valid_tickers[0] if valid_tickers else None
    
    def extract_nav_from_page(self, page_text: str) -> Optional[float]:
        """Извлекает СЧА со страницы"""
        
        # Паттерны для поиска СЧА
        nav_patterns = [
            r'СЧА[:\s]*([0-9,.\s]+)',
            r'чистых активов[:\s]*([0-9,.\s]+)',
            r'активы[:\s]*([0-9,.\s]+)',
            r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # Большие числа с запятыми
        ]
        
        for pattern in nav_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                try:
                    # Очищаем число
                    cleaned = re.sub(r'[^\d.]', '', match.replace(',', ''))
                    if cleaned:
                        value = float(cleaned)
                        # СЧА должна быть больше 1 млн рублей
                        if value > 1_000_000:
                            return value
                except ValueError:
                    continue
        
        return None
    
    def check_fund_id(self, fund_id: int) -> Optional[Dict]:
        """Проверяет конкретный ID фонда"""
        
        try:
            url = f"{self.base_url}/funds/{fund_id}/"
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text()
            
            # Извлекаем данные
            ticker = self.extract_ticker_from_page(soup, page_text)
            nav = self.extract_nav_from_page(page_text)
            
            # Название фонда
            title = soup.find('h1')
            fund_name = title.get_text(strip=True) if title else 'Unknown'
            
            if ticker:  # Найден потенциальный тикер
                fund_data = {
                    'fund_id': fund_id,
                    'ticker': ticker,
                    'name': fund_name,
                    'nav': nav or 0,
                    'url': url,
                    'page_length': len(page_text)
                }
                
                with self.lock:
                    print(f"✅ ID {fund_id}: {ticker} - {fund_name[:50]}... (СЧА: {nav:,.0f})" if nav else f"⚠️ ID {fund_id}: {ticker} - {fund_name[:50]}... (СЧА: не найдена)")
                
                return fund_data
            
        except Exception as e:
            if fund_id % 1000 == 0:  # Логируем только каждую 1000 ошибку
                with self.lock:
                    print(f"❌ Ошибка ID {fund_id}: {e}")
        
        return None
    
    def scan_id_range(self, start_id: int, end_id: int) -> List[Dict]:
        """Сканирует диапазон ID с многопоточностью"""
        
        print(f"🔍 Сканируем ID от {start_id} до {end_id} ({end_id - start_id + 1} фондов)")
        
        found_funds = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Создаем задачи для всех ID
            futures = {executor.submit(self.check_fund_id, fund_id): fund_id 
                      for fund_id in range(start_id, end_id + 1)}
            
            # Обрабатываем результаты
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                fund_data = future.result()
                if fund_data:
                    found_funds.append(fund_data)
                
                completed += 1
                if completed % 500 == 0:
                    print(f"📊 Проверено {completed}/{len(futures)} ID, найдено {len(found_funds)} фондов")
        
        return found_funds
    
    def match_with_our_data(self, found_funds: List[Dict], etf_data: pd.DataFrame) -> Dict[str, int]:
        """Сопоставляет найденные фонды с нашими данными"""
        
        mappings = {}
        unmatched_our = set(etf_data['ticker'].tolist())
        unmatched_found = []
        
        print(f"\n🔄 Сопоставляем {len(found_funds)} найденных фондов с {len(etf_data)} в нашей базе")
        
        for fund in found_funds:
            ticker = fund['ticker']
            fund_id = fund['fund_id']
            
            # Прямое совпадение тикера
            if ticker in unmatched_our:
                mappings[ticker] = fund_id
                unmatched_our.remove(ticker)
                print(f"✅ Прямое совпадение: {ticker} -> ID {fund_id}")
            else:
                unmatched_found.append(fund)
        
        print(f"\n📊 Результаты сопоставления:")
        print(f"  ✅ Прямых совпадений: {len(mappings)}")
        print(f"  ❌ Не найдено в нашей базе: {len(unmatched_found)}")
        print(f"  ❓ Не найдено на investfunds.ru: {len(unmatched_our)}")
        
        if unmatched_our:
            print(f"\n❓ Наши фонды без совпадений: {sorted(list(unmatched_our))[:10]}...")
        
        if unmatched_found:
            print(f"\n❌ Найденные фонды без совпадений:")
            for fund in unmatched_found[:5]:
                print(f"  {fund['ticker']}: {fund['name'][:40]}...")
        
        return mappings
    
    def smart_scan(self, etf_data: pd.DataFrame, known_good_ids: List[int] = None) -> Dict[str, int]:
        """Умное сканирование с фокусом на перспективные диапазоны"""
        
        all_mappings = {}
        
        # Добавляем известные хорошие маппинги
        if known_good_ids:
            print(f"🎯 Проверяем {len(known_good_ids)} известных ID...")
            known_funds = []
            for fund_id in known_good_ids:
                fund_data = self.check_fund_id(fund_id)
                if fund_data:
                    known_funds.append(fund_data)
            
            known_mappings = self.match_with_our_data(known_funds, etf_data)
            all_mappings.update(known_mappings)
        
        # Определяем перспективные диапазоны на основе известных ID
        if known_good_ids:
            min_id = min(known_good_ids)
            max_id = max(known_good_ids)
            
            # Сканируем диапазон вокруг известных ID
            scan_ranges = [
                (max(1, min_id - 500), min_id - 1),           # До минимального
                (min_id + 1, max_id - 1),                     # Между известными
                (max_id + 1, min(max_id + 2000, 15000))      # После максимального
            ]
        else:
            # Стандартные диапазоны
            scan_ranges = [
                (1, 2000),      # Старые фонды
                (5000, 8000),   # Средние
                (10000, 13000)  # Новые
            ]
        
        # Сканируем каждый диапазон
        for start_id, end_id in scan_ranges:
            print(f"\n🔍 Сканируем диапазон {start_id}-{end_id}")
            
            found_funds = self.scan_id_range(start_id, end_id)
            new_mappings = self.match_with_our_data(found_funds, etf_data)
            all_mappings.update(new_mappings)
            
            print(f"📈 Добавлено новых маппингов: {len(new_mappings)}")
            print(f"📊 Всего маппингов: {len(all_mappings)}")
            
            # Пауза между диапазонами
            time.sleep(2)
        
        return all_mappings

def main():
    """Основная функция"""
    
    # Загружаем данные ETF
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if not data_files:
        data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
    
    if not data_files:
        print("❌ Файлы с данными ETF не найдены")
        return
    
    latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
    etf_data = pd.read_csv(latest_data)
    
    print(f"📊 Загружено {len(etf_data)} фондов")
    
    # Создаем сканер
    scanner = SystematicFundDiscovery(max_workers=3)  # Не слишком агрессивно
    
    # Известные хорошие ID
    known_ids = [5973, 6225, 10147, 11703, 11499]  # LQDT, AKMB, золото, и др.
    
    print(f"🚀 Запускаем систематическое обнаружение фондов")
    print(f"🎯 Целевое количество: {len(etf_data)} фондов")
    
    # Умное сканирование
    final_mappings = scanner.smart_scan(etf_data, known_ids)
    
    # Сохраняем результаты
    results = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'total_etf_funds': len(etf_data),
        'mapped_funds': len(final_mappings),
        'coverage_percent': len(final_mappings) / len(etf_data) * 100,
        'mappings': final_mappings
    }
    
    with open('systematic_discovery_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n🎉 ИТОГИ СИСТЕМАТИЧЕСКОГО ОБНАРУЖЕНИЯ:")
    print(f"  📊 Всего наших фондов: {len(etf_data)}")
    print(f"  ✅ Найдено маппингов: {len(final_mappings)}")
    print(f"  📈 Покрытие: {len(final_mappings)/len(etf_data)*100:.1f}%")
    print(f"  💾 Результаты сохранены в systematic_discovery_results.json")
    
    if final_mappings:
        print(f"\n🏆 Найденные маппинги:")
        for ticker, fund_id in sorted(final_mappings.items()):
            print(f"  '{ticker}': {fund_id},")

if __name__ == "__main__":
    main()