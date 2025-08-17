#!/usr/bin/env python3
"""
Поиск фондов по тикерам на investfunds.ru через поисковую систему сайта
Более эффективный подход вместо сканирования всех ID
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import Dict, List, Optional
import time
import json
from pathlib import Path
import logging

class TickerBasedFundFinder:
    """Поиск фондов по тикерам через поисковую систему investfunds.ru"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://investfunds.ru"
        self.logger = self._setup_logger()
        self.found_mappings = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('TickerFundFinder')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def search_fund_by_ticker(self, ticker: str) -> List[Dict]:
        """Поиск фонда по тикеру через поисковую систему"""
        
        search_results = []
        
        try:
            # Различные варианты поиска
            search_queries = [
                ticker,
                f"БПИФ {ticker}",
                f"ETF {ticker}",
                f"фонд {ticker}"
            ]
            
            for query in search_queries:
                self.logger.info(f"🔍 Ищем '{query}' на investfunds.ru")
                
                # Поиск через основную поисковую строку
                search_url = f"{self.base_url}/search/"
                
                # Отправляем POST запрос как делает сайт
                search_data = {
                    'q': query,
                    'submit': 'Поиск'
                }
                
                response = self.session.get(f"{search_url}?q={query}", timeout=15)
                
                if response.status_code != 200:
                    self.logger.warning(f"Ошибка поиска '{query}': статус {response.status_code}")
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем результаты поиска
                results = self._extract_search_results(soup, ticker, query)
                search_results.extend(results)
                
                time.sleep(1)  # Пауза между запросами
                
                if results:
                    break  # Если нашли результаты, не продолжаем поиск
            
            # Дедуплицируем результаты
            unique_results = self._deduplicate_results(search_results)
            
            return unique_results
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска {ticker}: {e}")
            return []
    
    def _extract_search_results(self, soup: BeautifulSoup, ticker: str, query: str) -> List[Dict]:
        """Извлекает результаты поиска со страницы"""
        
        results = []
        
        # Паттерны для поиска ссылок на фонды
        fund_link_patterns = [
            r'/funds/(\d+)/',
            r'/fund/(\d+)/',
            r'/pif/(\d+)/'
        ]
        
        # Ищем все ссылки
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            link_text = link.get_text(strip=True)
            
            # Проверяем, является ли это ссылкой на фонд
            for pattern in fund_link_patterns:
                match = re.search(pattern, href)
                if match:
                    fund_id = int(match.group(1))
                    
                    # Проверяем релевантность
                    relevance_score = self._calculate_relevance(link_text, ticker, query)
                    
                    if relevance_score > 0.1:  # Минимальный порог релевантности
                        results.append({
                            'fund_id': fund_id,
                            'name': link_text,
                            'url': f"{self.base_url}{href}" if not href.startswith('http') else href,
                            'relevance': relevance_score,
                            'search_query': query
                        })
        
        # Также ищем в тексте страницы упоминания тикера с ID
        page_text = soup.get_text()
        ticker_mentions = self._find_ticker_with_ids(page_text, ticker)
        
        for mention in ticker_mentions:
            if not any(r['fund_id'] == mention['fund_id'] for r in results):
                results.append(mention)
        
        self.logger.info(f"   Найдено {len(results)} потенциальных совпадений для '{query}'")
        
        return results
    
    def _calculate_relevance(self, text: str, ticker: str, query: str) -> float:
        """Вычисляет релевантность найденного результата"""
        
        text_upper = text.upper()
        ticker_upper = ticker.upper()
        
        relevance = 0.0
        
        # Прямое совпадение тикера
        if ticker_upper in text_upper:
            relevance += 0.8
        
        # Ключевые слова
        keywords = ['БПИФ', 'ETF', 'фонд', 'индекс', ticker_upper]
        for keyword in keywords:
            if keyword.upper() in text_upper:
                relevance += 0.1
        
        # Штраф за нерелевантные слова
        irrelevant_words = ['новости', 'статья', 'аналитика', 'блог']
        for word in irrelevant_words:
            if word.upper() in text_upper:
                relevance -= 0.3
        
        return max(0.0, min(1.0, relevance))
    
    def _find_ticker_with_ids(self, text: str, ticker: str) -> List[Dict]:
        """Ищет упоминания тикера рядом с ID фондов в тексте"""
        
        results = []
        
        # Паттерны для поиска "тикер + ID"
        patterns = [
            rf'{ticker}\s*.*?/funds/(\d+)',
            rf'/funds/(\d+)\s*.*?{ticker}',
            rf'{ticker}.*?(\d{{4,6}})',  # Тикер + 4-6 цифр (возможный ID)
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                try:
                    fund_id = int(match.group(1))
                    if 1 <= fund_id <= 50000:  # Разумный диапазон ID
                        results.append({
                            'fund_id': fund_id,
                            'name': f'Найден по паттерну: {ticker}',
                            'url': f"{self.base_url}/funds/{fund_id}/",
                            'relevance': 0.7,
                            'search_query': f'pattern:{ticker}'
                        })
                except ValueError:
                    continue
        
        return results
    
    def _deduplicate_results(self, results: List[Dict]) -> List[Dict]:
        """Удаляет дубликаты и сортирует по релевантности"""
        
        seen_ids = set()
        unique_results = []
        
        # Сортируем по релевантности
        sorted_results = sorted(results, key=lambda x: x['relevance'], reverse=True)
        
        for result in sorted_results:
            if result['fund_id'] not in seen_ids:
                seen_ids.add(result['fund_id'])
                unique_results.append(result)
        
        return unique_results
    
    def verify_and_get_nav_data(self, fund_id: int, ticker: str) -> Optional[Dict]:
        """Проверяет найденный фонд и получает данные СЧА"""
        
        try:
            from investfunds_parser import InvestFundsParser
            parser = InvestFundsParser()
            
            # Получаем данные фонда
            fund_data = parser.get_fund_data(fund_id, use_cache=False)
            
            if fund_data and fund_data.get('nav', 0) > 0:
                # Дополнительная проверка - содержит ли страница тикер
                url = f"{self.base_url}/funds/{fund_id}/"
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    page_text = response.text.upper()
                    ticker_found = ticker.upper() in page_text
                    
                    # Ищем также альтернативные варианты написания
                    alt_variations = [
                        f"БПИФ {ticker}",
                        f"ETF {ticker}",
                        f"{ticker} ",
                        f" {ticker}",
                    ]
                    
                    for variation in alt_variations:
                        if variation.upper() in page_text:
                            ticker_found = True
                            break
                    
                    if ticker_found:
                        self.logger.info(f"✅ Подтверждено: {ticker} -> ID {fund_id} (СЧА: {fund_data['nav']/1e9:.1f} млрд ₽)")
                        return {
                            'ticker': ticker,
                            'fund_id': fund_id,
                            'nav': fund_data['nav'],
                            'unit_price': fund_data['unit_price'],
                            'name': fund_data['name'],
                            'verified': True,
                            'url': url
                        }
                    else:
                        self.logger.warning(f"⚠️ ID {fund_id} не содержит тикер {ticker}")
                        return None
                else:
                    self.logger.warning(f"⚠️ Не удалось проверить страницу ID {fund_id}")
                    return None
            else:
                self.logger.warning(f"⚠️ ID {fund_id} не содержит данных СЧА")
                return None
                
        except Exception as e:
            self.logger.error(f"Ошибка проверки ID {fund_id} для {ticker}: {e}")
            return None
    
    def find_all_funds_by_tickers(self, etf_data: pd.DataFrame, max_funds: int = None) -> Dict[str, Dict]:
        """Ищет все фонды по их тикерам"""
        
        self.logger.info(f"🎯 Начинаем поиск по тикерам для {len(etf_data)} фондов")
        
        results = {}
        processed_count = 0
        
        for idx, row in etf_data.iterrows():
            if max_funds and processed_count >= max_funds:
                break
                
            ticker = row['ticker']
            processed_count += 1
            
            self.logger.info(f"\n🔍 [{processed_count}/{len(etf_data)}] Ищем {ticker}")
            
            # Поиск возможных совпадений
            search_results = self.search_fund_by_ticker(ticker)
            
            if not search_results:
                self.logger.warning(f"❌ {ticker}: не найден в поиске")
                continue
            
            # Проверяем найденные результаты по убыванию релевантности
            verified_data = None
            
            for result in search_results[:3]:  # Проверяем топ-3 результата
                fund_id = result['fund_id']
                
                self.logger.info(f"   🧪 Проверяем ID {fund_id} (релевантность: {result['relevance']:.2f})")
                
                verified_data = self.verify_and_get_nav_data(fund_id, ticker)
                
                if verified_data:
                    results[ticker] = verified_data
                    self.found_mappings[ticker] = fund_id
                    break
                
                time.sleep(0.5)  # Пауза между проверками
            
            if not verified_data:
                self.logger.warning(f"❌ {ticker}: найдены совпадения, но не подтверждены")
            
            time.sleep(2)  # Пауза между поисками разных тикеров
        
        self.logger.info(f"\n🎉 Поиск завершен:")
        self.logger.info(f"   ✅ Найдено и подтверждено: {len(results)} фондов")
        self.logger.info(f"   📊 Покрытие: {len(results)/len(etf_data)*100:.1f}%")
        
        return results
    
    def save_results(self, results: Dict, filename: str = "ticker_search_results.json"):
        """Сохраняет результаты поиска"""
        
        save_data = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'total_found': len(results),
            'fund_mappings': self.found_mappings,
            'detailed_results': results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"💾 Результаты сохранены в {filename}")
    
    def update_investfunds_parser_mapping(self, results: Dict):
        """Обновляет маппинг в investfunds_parser.py"""
        
        try:
            # Читаем текущий файл
            parser_file = Path("investfunds_parser.py")
            if not parser_file.exists():
                self.logger.error("❌ Файл investfunds_parser.py не найден")
                return
            
            with open(parser_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Ищем секцию с маппингом
            mapping_start = content.find("self.fund_mapping = {")
            mapping_end = content.find("}", mapping_start) + 1
            
            if mapping_start == -1 or mapping_end == -1:
                self.logger.error("❌ Не найдена секция fund_mapping в файле")
                return
            
            # Создаем новый маппинг, объединяя старый и новый
            new_mapping_lines = []
            
            # Добавляем комментарий
            new_mapping_lines.append("        # Маппинг тикеров на ID фондов (автоматически обновлено)")
            new_mapping_lines.append("        self.fund_mapping = {")
            
            # Добавляем найденные маппинги
            for ticker, data in results.items():
                fund_id = data['fund_id']
                comment = f"  # {data['name'][:50]}..." if len(data['name']) > 50 else f"  # {data['name']}"
                new_mapping_lines.append(f"            '{ticker}': {fund_id},{comment}")
            
            new_mapping_lines.append("        }")
            
            # Заменяем старый маппинг новым
            new_content = (
                content[:mapping_start] + 
                "\n".join(new_mapping_lines) + 
                content[mapping_end:]
            )
            
            # Создаем бэкап
            backup_file = f"investfunds_parser_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.py"
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Записываем обновленный файл
            with open(parser_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            self.logger.info(f"✅ Обновлен маппинг в investfunds_parser.py ({len(results)} фондов)")
            self.logger.info(f"📄 Бэкап сохранен как {backup_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления маппинга: {e}")

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
    
    print(f"📊 Загружено {len(etf_data)} ETF для поиска по тикерам")
    
    # Создаем поисковик
    finder = TickerBasedFundFinder()
    
    print(f"\n🚀 Запускаем поиск по тикерам (тестируем первые 30 фондов)")
    
    # Запускаем поиск
    results = finder.find_all_funds_by_tickers(etf_data.head(30), max_funds=30)
    
    # Сохраняем результаты
    finder.save_results(results)
    
    if results:
        print(f"\n✅ Найденные маппинги:")
        for ticker, data in results.items():
            print(f"   {ticker} -> ID {data['fund_id']} (СЧА: {data['nav']/1e9:.1f} млрд ₽)")
        
        # Обновляем investfunds_parser.py
        finder.update_investfunds_parser_mapping(results)
        
        print(f"\n🎯 Итоги:")
        print(f"   📊 Найдено: {len(results)} из 30 фондов")
        print(f"   📈 Успешность: {len(results)/30*100:.1f}%")
    else:
        print("❌ Не удалось найти ни одного фонда")
    
    print(f"\n💾 Результаты сохранены в ticker_search_results.json")

if __name__ == "__main__":
    main()