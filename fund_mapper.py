#!/usr/bin/env python3
"""
Автоматический маппер БПИФ на investfunds.ru
Находит ID фондов по названию, ISIN и другим параметрам
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
from urllib.parse import quote
import difflib
from dataclasses import dataclass

@dataclass
class FundMatch:
    """Результат поиска фонда"""
    fund_id: Optional[int]
    confidence: float  # 0-1, уверенность в совпадении
    name_match: str
    url: str
    reason: str

class FundMapper:
    """Автоматический маппер БПИФ на investfunds.ru"""
    
    def __init__(self, output_file: str = "fund_mapping.json"):
        self.base_url = "https://investfunds.ru"
        self.output_file = Path(output_file)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        self.logger = self._setup_logger()
        
        # Загружаем существующий маппинг
        self.mapping = self._load_existing_mapping()
        
        # Кеш для поиска
        self.search_cache = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('FundMapper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _load_existing_mapping(self) -> Dict[str, int]:
        """Загружает существующий маппинг"""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('mapping', {})
            except Exception as e:
                self.logger.warning(f"Ошибка загрузки маппинга: {e}")
        
        # Известные маппинги
        return {
            'LQDT': 5973,
        }
    
    def save_mapping(self):
        """Сохраняет маппинг в файл"""
        data = {
            'mapping': self.mapping,
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_funds': len(self.mapping)
        }
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Маппинг сохранен: {len(self.mapping)} фондов")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения маппинга: {e}")
    
    def search_fund_by_name(self, fund_name: str) -> List[FundMatch]:
        """Ищет фонд по названию"""
        
        # Проверяем кеш
        cache_key = fund_name.lower().strip()
        if cache_key in self.search_cache:
            return self.search_cache[cache_key]
        
        matches = []
        
        try:
            # Очищаем название для поиска
            clean_name = self._clean_fund_name(fund_name)
            search_query = quote(clean_name)
            
            # Поиск через страницу со списком фондов
            funds_url = f"{self.base_url}/funds/"
            
            self.logger.info(f"Поиск фонда: {fund_name} -> {clean_name}")
            
            response = self.session.get(funds_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем ссылки на фонды
            fund_links = soup.find_all('a', href=re.compile(r'/funds/\d+/'))
            
            for link in fund_links:
                try:
                    # Извлекаем ID фонда из URL
                    href = link.get('href', '')
                    fund_id_match = re.search(r'/funds/(\d+)/', href)
                    
                    if not fund_id_match:
                        continue
                    
                    fund_id = int(fund_id_match.group(1))
                    link_text = link.get_text(strip=True)
                    
                    if not link_text:
                        continue
                    
                    # Вычисляем схожесть названий
                    confidence = self._calculate_name_similarity(clean_name, link_text)
                    
                    if confidence > 0.3:  # Минимальная схожесть
                        matches.append(FundMatch(
                            fund_id=fund_id,
                            confidence=confidence,
                            name_match=link_text,
                            url=f"{self.base_url}{href}",
                            reason=f"Схожесть названия: {confidence:.2f}"
                        ))
                
                except Exception as e:
                    self.logger.debug(f"Ошибка обработки ссылки {link}: {e}")
                    continue
            
            # Сортируем по уверенности
            matches.sort(key=lambda x: x.confidence, reverse=True)
            
            # Кешируем результат
            self.search_cache[cache_key] = matches[:5]  # Топ-5 совпадений
            
            return matches[:5]
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска фонда {fund_name}: {e}")
            return []
    
    def search_fund_by_ticker_pattern(self, ticker: str, fund_name: str) -> List[FundMatch]:
        """Ищет фонд по паттернам тикера в названии"""
        
        matches = []
        
        # Создаем поисковые паттерны
        patterns = [
            ticker.upper(),
            ticker.lower(),
            f"({ticker.upper()})",
            f", {ticker.upper()}",
            f" {ticker.upper()} ",
        ]
        
        try:
            # Поиск в базе фондов
            funds_url = f"{self.base_url}/funds/"
            response = self.session.get(funds_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text()
            
            # Ищем тикер в тексте страницы
            for pattern in patterns:
                if pattern in page_text:
                    # Находим контекст вокруг тикера
                    pattern_pos = page_text.find(pattern)
                    if pattern_pos != -1:
                        context_start = max(0, pattern_pos - 100)
                        context_end = min(len(page_text), pattern_pos + 100)
                        context = page_text[context_start:context_end]
                        
                        # Ищем ссылки на фонды в контексте
                        context_soup = BeautifulSoup(context, 'html.parser')
                        fund_links = context_soup.find_all('a', href=re.compile(r'/funds/\d+/'))
                        
                        for link in fund_links:
                            href = link.get('href', '')
                            fund_id_match = re.search(r'/funds/(\d+)/', href)
                            
                            if fund_id_match:
                                fund_id = int(fund_id_match.group(1))
                                
                                matches.append(FundMatch(
                                    fund_id=fund_id,
                                    confidence=0.8,  # Высокая уверенность при совпадении тикера
                                    name_match=link.get_text(strip=True),
                                    url=f"{self.base_url}{href}",
                                    reason=f"Найден тикер {pattern} в контексте"
                                ))
                                break
        
        except Exception as e:
            self.logger.error(f"Ошибка поиска по тикеру {ticker}: {e}")
        
        return matches
    
    def _clean_fund_name(self, name: str) -> str:
        """Очищает название фонда для поиска"""
        
        # Убираем общие префиксы и суффиксы
        clean = name
        
        # Убираем упоминания "БПИФ", "ETF", "Фонд"
        clean = re.sub(r'\b(БПИФ|ETF|Фонд)\b', '', clean, flags=re.IGNORECASE)
        
        # Убираем кавычки и скобки с содержимым
        clean = re.sub(r'[«»"\'()].*?[«»"\'()]', '', clean)
        clean = re.sub(r'\([^)]*\)', '', clean)
        
        # Убираем лишние пробелы
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        return clean
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Вычисляет схожесть названий"""
        
        # Нормализуем названия
        name1_clean = name1.lower().strip()
        name2_clean = name2.lower().strip()
        
        # Используем SequenceMatcher для расчета схожести
        similarity = difflib.SequenceMatcher(None, name1_clean, name2_clean).ratio()
        
        # Бонус за ключевые слова
        keywords1 = set(re.findall(r'\b\w{3,}\b', name1_clean))
        keywords2 = set(re.findall(r'\b\w{3,}\b', name2_clean))
        
        if keywords1 and keywords2:
            keyword_overlap = len(keywords1 & keywords2) / len(keywords1 | keywords2)
            similarity = max(similarity, keyword_overlap)
        
        return similarity
    
    def verify_fund_match(self, fund_id: int, expected_ticker: str, expected_name: str) -> bool:
        """Проверяет, что фонд действительно соответствует ожиданиям"""
        
        try:
            from investfunds_parser import InvestFundsParser
            parser = InvestFundsParser()
            
            fund_data = parser.get_fund_data(fund_id, use_cache=False)
            
            if not fund_data:
                return False
            
            fund_name = fund_data.get('name', '')
            
            # Проверяем наличие тикера в названии
            if expected_ticker.upper() in fund_name.upper():
                return True
            
            # Проверяем схожесть названий
            similarity = self._calculate_name_similarity(expected_name, fund_name)
            
            return similarity > 0.6
            
        except Exception as e:
            self.logger.error(f"Ошибка проверки фонда {fund_id}: {e}")
            return False
    
    def map_all_funds(self, etf_data: pd.DataFrame, auto_confirm: bool = False) -> Dict[str, int]:
        """Маппит все фонды автоматически"""
        
        self.logger.info(f"Начинаем маппинг {len(etf_data)} фондов...")
        
        successful_mappings = 0
        
        for idx, row in etf_data.iterrows():
            ticker = row['ticker']
            fund_name = row.get('full_name', row.get('short_name', ticker))
            
            # Пропускаем уже замапленные фонды
            if ticker in self.mapping:
                self.logger.info(f"✅ {ticker}: уже замаплен (ID: {self.mapping[ticker]})")
                continue
            
            self.logger.info(f"\n🔍 Ищем: {ticker} - {fund_name}")
            
            # Поиск по названию
            name_matches = self.search_fund_by_name(fund_name)
            
            # Поиск по тикеру
            ticker_matches = self.search_fund_by_ticker_pattern(ticker, fund_name)
            
            # Объединяем результаты
            all_matches = name_matches + ticker_matches
            
            # Убираем дубликаты
            seen_ids = set()
            unique_matches = []
            for match in all_matches:
                if match.fund_id and match.fund_id not in seen_ids:
                    seen_ids.add(match.fund_id)
                    unique_matches.append(match)
            
            # Сортируем по уверенности
            unique_matches.sort(key=lambda x: x.confidence, reverse=True)
            
            if unique_matches:
                best_match = unique_matches[0]
                
                self.logger.info(f"📋 Найдено совпадений: {len(unique_matches)}")
                self.logger.info(f"🏆 Лучшее совпадение: ID {best_match.fund_id}, уверенность {best_match.confidence:.2f}")
                self.logger.info(f"   Название: {best_match.name_match}")
                self.logger.info(f"   Причина: {best_match.reason}")
                
                # Автоматическое подтверждение для высокой уверенности
                if auto_confirm and best_match.confidence > 0.8:
                    self.mapping[ticker] = best_match.fund_id
                    successful_mappings += 1
                    self.logger.info(f"✅ Автоматически замаплен: {ticker} -> {best_match.fund_id}")
                
                elif not auto_confirm:
                    # Интерактивное подтверждение
                    print(f"\n🔍 Фонд: {ticker} - {fund_name}")
                    print(f"🏆 Предлагаемый ID: {best_match.fund_id}")
                    print(f"📊 Уверенность: {best_match.confidence:.2f}")
                    print(f"📝 Название на сайте: {best_match.name_match}")
                    print(f"🔗 URL: {best_match.url}")
                    
                    # Показываем альтернативы
                    if len(unique_matches) > 1:
                        print("\n📋 Альтернативы:")
                        for i, match in enumerate(unique_matches[1:4], 2):
                            print(f"   {i}. ID {match.fund_id}: {match.name_match} (уверенность: {match.confidence:.2f})")
                    
                    choice = input("\nПодтверждаете маппинг? (y/n/id_number): ").strip().lower()
                    
                    if choice == 'y':
                        self.mapping[ticker] = best_match.fund_id
                        successful_mappings += 1
                        self.logger.info(f"✅ Подтвержден маппинг: {ticker} -> {best_match.fund_id}")
                    elif choice.isdigit():
                        custom_id = int(choice)
                        self.mapping[ticker] = custom_id
                        successful_mappings += 1
                        self.logger.info(f"✅ Ручной маппинг: {ticker} -> {custom_id}")
                    else:
                        self.logger.info(f"❌ Пропущен: {ticker}")
            else:
                self.logger.warning(f"❌ Не найдено совпадений для: {ticker}")
            
            # Пауза между запросами
            time.sleep(1)
            
            # Сохраняем прогресс каждые 10 фондов
            if (idx + 1) % 10 == 0:
                self.save_mapping()
                self.logger.info(f"💾 Промежуточное сохранение: {successful_mappings}/{idx+1}")
        
        # Финальное сохранение
        self.save_mapping()
        
        self.logger.info(f"\n🎉 Маппинг завершен!")
        self.logger.info(f"✅ Успешно замаплено: {successful_mappings}")
        self.logger.info(f"❌ Не найдено: {len(etf_data) - successful_mappings}")
        self.logger.info(f"📊 Процент успеха: {successful_mappings/len(etf_data)*100:.1f}%")
        
        return self.mapping

def main():
    """Основная функция для запуска маппинга"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Автоматический маппинг БПИФ на investfunds.ru')
    parser.add_argument('--auto', action='store_true', help='Автоматическое подтверждение для высокой уверенности')
    parser.add_argument('--data-file', help='Путь к файлу с данными ETF')
    
    args = parser.parse_args()
    
    # Загружаем данные ETF
    if args.data_file:
        data_file = args.data_file
    else:
        # Ищем последний файл с данными
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
    mapper = FundMapper()
    
    print(f"\n🚀 Начинаем автоматический маппинг всех {len(etf_data)} фондов")
    print(f"⚙️  Режим: {'Автоматический' if args.auto else 'Интерактивный'}")
    
    # Запускаем маппинг
    final_mapping = mapper.map_all_funds(etf_data, auto_confirm=args.auto)
    
    print(f"\n📋 Финальная статистика:")
    print(f"   Всего фондов: {len(etf_data)}")
    print(f"   Замаплено: {len(final_mapping)}")
    print(f"   Не найдено: {len(etf_data) - len(final_mapping)}")
    print(f"   Файл маппинга: fund_mapping.json")

if __name__ == "__main__":
    main()