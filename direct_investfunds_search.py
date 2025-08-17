#!/usr/bin/env python3
"""
Прямой поиск фондов на investfunds.ru по тикеру и проверка всех возможных ID
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import Dict, List, Optional
import time
from pathlib import Path
import json

class DirectInvestFundsSearch:
    """Прямой поиск на investfunds.ru"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://investfunds.ru"
        
    def search_by_ticker(self, ticker: str) -> List[Dict]:
        """Ищет фонд по тикеру через поисковую строку"""
        
        results = []
        
        try:
            # Поиск через основную поисковую строку
            search_url = f"{self.base_url}/search/?q={ticker}"
            
            response = self.session.get(search_url, timeout=15)
            if response.status_code != 200:
                return results
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем ссылки на фонды в результатах поиска
            fund_links = soup.find_all('a', href=re.compile(r'/funds/\d+/'))
            
            for link in fund_links:
                href = link.get('href', '')
                fund_id_match = re.search(r'/funds/(\d+)/', href)
                
                if fund_id_match:
                    fund_id = int(fund_id_match.group(1))
                    link_text = link.get_text(strip=True)
                    
                    # Проверяем, что ссылка содержит тикер
                    if ticker.upper() in link_text.upper():
                        results.append({
                            'fund_id': fund_id,
                            'name': link_text,
                            'url': f"{self.base_url}{href}",
                            'confidence': 0.9,
                            'source': 'search'
                        })
            
            # Поиск в тексте страницы
            page_text = soup.get_text()
            ticker_mentions = page_text.upper().count(ticker.upper())
            
            if ticker_mentions > 0:
                print(f"🔍 Тикер {ticker} упоминается {ticker_mentions} раз на странице поиска")
                
                # Ищем ID рядом с упоминаниями тикера
                pattern = rf'{ticker.upper()}.*?/funds/(\d+)/'
                id_matches = re.findall(pattern, page_text.upper())
                
                for found_id in id_matches:
                    if not any(r['fund_id'] == int(found_id) for r in results):
                        results.append({
                            'fund_id': int(found_id),
                            'name': f'Found via pattern {ticker}',
                            'url': f"{self.base_url}/funds/{found_id}/",
                            'confidence': 0.7,
                            'source': 'pattern'
                        })
            
        except Exception as e:
            print(f"Ошибка поиска {ticker}: {e}")
        
        return results
    
    def scan_fund_id_range(self, start_id: int = 1, end_id: int = 15000, ticker_filter: str = None) -> List[Dict]:
        """Сканирует диапазон ID фондов для поиска нужного тикера"""
        
        found_funds = []
        
        print(f"🔍 Сканируем ID фондов от {start_id} до {end_id}")
        
        for fund_id in range(start_id, end_id + 1):
            try:
                url = f"{self.base_url}/funds/{fund_id}/"
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    page_text = soup.get_text().upper()
                    
                    # Если указан фильтр по тикеру
                    if ticker_filter and ticker_filter.upper() in page_text:
                        
                        # Извлекаем название фонда
                        title = soup.find('h1')
                        fund_name = title.get_text(strip=True) if title else 'Unknown'
                        
                        # Ищем СЧА
                        nav_pattern = r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
                        nav_matches = re.findall(nav_pattern, page_text)
                        
                        nav_value = 0
                        for match in nav_matches:
                            try:
                                value = float(match.replace(',', ''))
                                if value > 1000000:  # Больше миллиона - похоже на СЧА
                                    nav_value = value
                                    break
                            except:
                                continue
                        
                        found_funds.append({
                            'fund_id': fund_id,
                            'ticker': ticker_filter,
                            'name': fund_name,
                            'nav': nav_value,
                            'url': url,
                            'confidence': 1.0
                        })
                        
                        print(f"✅ Найден {ticker_filter}: ID {fund_id}, СЧА {nav_value:,.0f}")
                    
                    # Если не указан фильтр, собираем все фонды
                    elif not ticker_filter:
                        title = soup.find('h1')
                        if title:
                            fund_name = title.get_text(strip=True)
                            found_funds.append({
                                'fund_id': fund_id,
                                'name': fund_name,
                                'url': url
                            })
                
                # Пауза между запросами
                if fund_id % 100 == 0:
                    print(f"📊 Проверено {fund_id} ID...")
                    time.sleep(1)
                
                time.sleep(0.1)
                
            except Exception as e:
                if fund_id % 1000 == 0:
                    print(f"⚠️ Ошибка на ID {fund_id}: {e}")
                continue
        
        return found_funds
    
    def test_known_funds(self) -> Dict[str, Dict]:
        """Тестирует поиск для известных фондов"""
        
        # Известные соответствия для тестирования
        known_funds = {
            'AKMB': 6225,  # Из вашего примера
            'LQDT': 5973,  # Уже знаем
        }
        
        results = {}
        
        for ticker, expected_id in known_funds.items():
            print(f"\n🧪 Тестируем {ticker} (ожидаемый ID: {expected_id})")
            
            # Прямая проверка известного ID
            url = f"{self.base_url}/funds/{expected_id}/"
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    page_text = soup.get_text()
                    
                    # Проверяем, есть ли тикер на странице
                    if ticker.upper() in page_text.upper():
                        
                        # Извлекаем СЧА
                        nav_patterns = [
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                            r'СЧА[:\s]*([0-9,.\s]+)',
                        ]
                        
                        nav_value = 0
                        for pattern in nav_patterns:
                            matches = re.findall(pattern, page_text)
                            for match in matches:
                                try:
                                    cleaned = re.sub(r'[^\d.]', '', match)
                                    value = float(cleaned)
                                    if value > 1000000:
                                        nav_value = value
                                        break
                                except:
                                    continue
                            if nav_value > 0:
                                break
                        
                        results[ticker] = {
                            'fund_id': expected_id,
                            'nav': nav_value,
                            'url': url,
                            'status': 'confirmed'
                        }
                        
                        print(f"  ✅ Подтвержден: СЧА {nav_value:,.0f}")
                    else:
                        print(f"  ❌ Тикер {ticker} не найден на странице ID {expected_id}")
                else:
                    print(f"  ❌ Страница ID {expected_id} недоступна")
                    
            except Exception as e:
                print(f"  ⚠️ Ошибка проверки {ticker}: {e}")
        
        return results

def main():
    """Основная функция для тестирования"""
    
    searcher = DirectInvestFundsSearch()
    
    print("🎯 ПРЯМОЙ ПОИСК НА INVESTFUNDS.RU")
    print("=" * 50)
    
    # 1. Тестируем известные фонды
    print("\n1️⃣ Тестирование известных фондов:")
    known_results = searcher.test_known_funds()
    
    # 2. Поиск по тикеру AKMB
    print("\n2️⃣ Поиск AKMB через поисковую строку:")
    akmb_search = searcher.search_by_ticker('AKMB')
    
    for result in akmb_search:
        print(f"  Найден: ID {result['fund_id']}, {result['name']}")
    
    # 3. Целевой поиск конкретного фонда
    print("\n3️⃣ Целевая проверка AKMB на ID 6225:")
    
    try:
        from investfunds_parser import InvestFundsParser
        parser = InvestFundsParser()
        
        # Добавляем AKMB в маппинг и тестируем
        akmb_data = parser.get_fund_data(6225, use_cache=False)
        
        if akmb_data:
            print(f"  ✅ Успешно получены данные:")
            print(f"     Название: {akmb_data['name']}")
            print(f"     СЧА: {akmb_data['nav']:,.2f} руб.")
            print(f"     Цена пая: {akmb_data['unit_price']:.4f} руб.")
        else:
            print(f"  ❌ Не удалось получить данные")
            
    except Exception as e:
        print(f"  ⚠️ Ошибка тестирования парсера: {e}")
    
    print(f"\n📊 Итоги:")
    print(f"  Известных фондов подтверждено: {len(known_results)}")
    print(f"  Результатов поиска AKMB: {len(akmb_search)}")

if __name__ == "__main__":
    main()