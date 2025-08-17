#!/usr/bin/env python3
"""
Ручной маппер фондов с использованием известных паттернов
Более эффективный подход для расширения покрытия
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json
from pathlib import Path
import time
from typing import Dict, List, Optional

class ManualFundMapper:
    """Ручной маппер для создания расширенного списка фондов"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://investfunds.ru"
        
        # Расширенный список известных и предполагаемых маппингов
        # Основан на анализе структуры ID и паттернов именования
        self.candidate_mappings = {
            # Подтвержденные
            'LQDT': 5973,   # ВИМ Ликвидность
            'AKMB': 6225,   # Альфа-Капитал Управляемые облигации  
            'AKGD': 10147,  # А-Капитал Золото
            
            # Альфа-Капитал фонды (вероятно в диапазоне 6000-7000, 10000-12000)
            'AKMM': 6200,   # Возможно денежный рынок
            'AKFT': 6250,   # Возможно флоатеры
            'AMGL': 12445,  # Глобальные ликвидности (уже в старом маппинге)
            'AMNR': 10053,  # Америка (уже в старом маппинге)
            'AMNY': 10613,  # Доллар США (уже в старом маппинге)
            'AMRE': 6809,   # Российские акции (уже в старом маппинге)
            
            # Сбербанк АМ фонды (возможно в диапазоне 1000-3000)
            'SBMM': 1500,   # Сбербанк денежный рынок
            'SBGB': 1600,   # Сбербанк гособлигации
            'SBCB': 1700,   # Сбербанк корпоративные облигации
            'SBER': 1800,   # Сбербанк акции
            
            # Т-Капитал (Тинькофф) фонды (возможно в диапазоне 8000-9000)
            'TMOS': 8100,   # Тинькофф Московская биржа
            'TGLD': 8200,   # Тинькофф золото
            'TBRU': 8300,   # Тинькофф рублевые облигации
            'TBEU': 8400,   # Тинькофф еврооблигации
            'TCSG': 8500,   # Тинькофф техсектор
            
            # ВТБ Капитал фонды (возможно в диапазоне 3000-4000)
            'VTBM': 3100,   # ВТБ денежный рынок
            'VTBR': 3200,   # ВТБ российские акции
            'VTBB': 3300,   # ВТБ облигации
            
            # АК БАРС фонды (возможно в диапазоне 11000-12000)
            'BCSD': 11499,  # АК БАРС денежный рынок (уже в старом маппинге)
            'MONY': 11500,  # Деньги
            'BCSG': 11600,  # АК БАРС гособлигации
            'BCSR': 11700,  # АК БАРС российские акции
        }
    
    def verify_fund_mapping(self, ticker: str, fund_id: int) -> Optional[Dict]:
        """Проверяет маппинг тикера на ID фонда"""
        
        try:
            print(f"🔍 Проверяем {ticker} -> ID {fund_id}")
            
            url = f"{self.base_url}/funds/{fund_id}/"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ {ticker}: страница недоступна (статус {response.status_code})")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text().upper()
            
            # Ищем тикер на странице
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
            
            # Получаем название фонда
            title = soup.find('h1')
            fund_name = title.get_text(strip=True) if title else 'Unknown'
            
            if ticker_found:
                # Пытаемся извлечь СЧА и цену пая
                nav_value = self._extract_nav_from_page(page_text)
                unit_price = self._extract_unit_price_from_page(page_text)
                
                print(f"✅ {ticker}: найден! СЧА: {nav_value/1e9 if nav_value else 0:.1f} млрд ₽")
                
                return {
                    'ticker': ticker,
                    'fund_id': fund_id,
                    'name': fund_name,
                    'nav': nav_value or 0,
                    'unit_price': unit_price or 0,
                    'url': url,
                    'verified': True
                }
            else:
                print(f"⚠️ {ticker}: ID {fund_id} существует, но не содержит тикер")
                print(f"   Название: {fund_name[:60]}...")
                return None
                
        except Exception as e:
            print(f"❌ {ticker}: ошибка проверки ID {fund_id}: {e}")
            return None
    
    def _extract_nav_from_page(self, page_text: str) -> Optional[float]:
        """Извлекает СЧА со страницы"""
        
        # Паттерны для поиска СЧА
        nav_patterns = [
            r'СЧА[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'чистых активов[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'Net Asset Value[:\s]*([0-9\s,]+(?:\.\d+)?)',
            r'(\d{1,3}(?:\s\d{3})+(?:\s\d{3})*(?:\.\d+)?)',  # Числа с пробелами как разделителями тысяч
        ]
        
        for pattern in nav_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                try:
                    # Очищаем число - убираем пробелы и заменяем запятые
                    cleaned = re.sub(r'[^\d.]', '', match.replace(' ', '').replace(',', ''))
                    if cleaned:
                        value = float(cleaned)
                        # СЧА должна быть больше 1 млн рублей
                        if value > 1_000_000:
                            return value
                except ValueError:
                    continue
        
        return None
    
    def _extract_unit_price_from_page(self, page_text: str) -> Optional[float]:
        """Извлекает цену пая со страницы"""
        
        price_patterns = [
            r'цена пая[:\s]*([0-9.,]+)',
            r'стоимость пая[:\s]*([0-9.,]+)',
            r'unit price[:\s]*([0-9.,]+)',
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                try:
                    cleaned = re.sub(r'[^\d.]', '', match.replace(',', '.'))
                    if cleaned:
                        value = float(cleaned)
                        if 0.1 <= value <= 10000:
                            return value
                except ValueError:
                    continue
        
        return None
    
    def verify_all_candidates(self) -> Dict[str, Dict]:
        """Проверяет все кандидаты на маппинг"""
        
        print(f"🎯 Проверяем {len(self.candidate_mappings)} кандидатов на маппинг")
        print("=" * 60)
        
        verified_mappings = {}
        
        for ticker, fund_id in self.candidate_mappings.items():
            result = self.verify_fund_mapping(ticker, fund_id)
            
            if result:
                verified_mappings[ticker] = result
            
            time.sleep(1)  # Пауза между запросами
            print()  # Пустая строка для читаемости
        
        return verified_mappings
    
    def create_priority_list(self, etf_data: pd.DataFrame) -> List[str]:
        """Создает приоритетный список тикеров для поиска"""
        
        # Сортируем по ликвидности/объему торгов
        if 'avg_daily_value_rub' in etf_data.columns:
            sorted_etf = etf_data.sort_values('avg_daily_value_rub', ascending=False)
        else:
            sorted_etf = etf_data
        
        return sorted_etf['ticker'].tolist()
    
    def interactive_mapping(self, etf_data: pd.DataFrame):
        """Интерактивный процесс маппинга"""
        
        priority_tickers = self.create_priority_list(etf_data)
        verified_mappings = {}
        
        print("🎯 ИНТЕРАКТИВНЫЙ МАППИНГ ФОНДОВ")
        print("Для каждого тикера предлагается несколько ID для проверки")
        print("=" * 60)
        
        for ticker in priority_tickers[:20]:  # Ограничиваем первыми 20
            if ticker in self.candidate_mappings:
                continue  # Уже проверили
            
            print(f"\n🔍 Ищем {ticker}")
            
            # Предлагаем ID на основе паттернов
            suggested_ids = self._suggest_ids_for_ticker(ticker)
            
            found = False
            for fund_id in suggested_ids:
                result = self.verify_fund_mapping(ticker, fund_id)
                if result:
                    verified_mappings[ticker] = result
                    found = True
                    break
                
                time.sleep(0.5)
            
            if not found:
                print(f"❌ {ticker}: не найден в предложенных ID")
        
        return verified_mappings
    
    def _suggest_ids_for_ticker(self, ticker: str) -> List[int]:
        """Предлагает вероятные ID для тикера на основе паттернов"""
        
        suggestions = []
        
        # Паттерны на основе префиксов тикеров
        if ticker.startswith('AK') or ticker.startswith('AM'):
            # Альфа-Капитал: диапазоны 6000-7000, 10000-12000
            suggestions.extend([6000 + i*50 for i in range(20)])  # 6000-6950
            suggestions.extend([10000 + i*100 for i in range(20)]) # 10000-11900
            
        elif ticker.startswith('SB'):
            # Сбербанк АМ: диапазон 1000-3000
            suggestions.extend([1000 + i*100 for i in range(20)])  # 1000-2900
            
        elif ticker.startswith('T'):
            # Тинькофф: диапазон 8000-9000
            suggestions.extend([8000 + i*50 for i in range(20)])   # 8000-8950
            
        elif ticker.startswith('VTB'):
            # ВТБ: диапазон 3000-4000
            suggestions.extend([3000 + i*50 for i in range(20)])   # 3000-3950
            
        elif ticker.startswith('BC') or ticker.startswith('MON'):
            # АК БАРС: диапазон 11000-12000
            suggestions.extend([11000 + i*50 for i in range(20)])  # 11000-11950
            
        else:
            # Общие диапазоны
            suggestions.extend([1000, 2000, 3000, 5000, 6000, 8000, 10000, 11000, 12000])
        
        return suggestions[:10]  # Ограничиваем 10 предложениями

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
    
    print(f"📊 Загружено {len(etf_data)} ETF")
    
    # Создаем маппер
    mapper = ManualFundMapper()
    
    print(f"\n🚀 Запускаем проверку кандидатов на маппинг")
    
    # Проверяем все кандидаты
    verified_results = mapper.verify_all_candidates()
    
    # Сохраняем результаты
    save_data = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'verified_mappings': len(verified_results),
        'total_candidates': len(mapper.candidate_mappings),
        'success_rate': len(verified_results) / len(mapper.candidate_mappings) * 100,
        'results': verified_results
    }
    
    with open('manual_mapping_results.json', 'w', encoding='utf-8') as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n🎉 РЕЗУЛЬТАТЫ РУЧНОГО МАППИНГА:")
    print(f"   📊 Проверено кандидатов: {len(mapper.candidate_mappings)}")
    print(f"   ✅ Подтверждено: {len(verified_results)}")
    print(f"   📈 Успешность: {len(verified_results)/len(mapper.candidate_mappings)*100:.1f}%")
    
    if verified_results:
        print(f"\n🏆 Подтвержденные маппинги:")
        verified_mapping = {}
        for ticker, data in verified_results.items():
            if data.get('nav', 0) > 0:
                verified_mapping[ticker] = data['fund_id']
                print(f"   {ticker} -> ID {data['fund_id']} (СЧА: {data['nav']/1e9:.1f} млрд ₽)")
        
        # Рассчитываем покрытие рынка
        total_nav = sum(data.get('nav', 0) for data in verified_results.values())
        print(f"\n📊 Покрытие рынка:")
        print(f"   💰 Общая СЧА найденных фондов: {total_nav/1e9:.1f} млрд ₽")
        print(f"   📈 Количество найденных фондов: {len(verified_mapping)}")
        
        print(f"\n📋 Маппинг для investfunds_parser.py:")
        for ticker, fund_id in sorted(verified_mapping.items()):
            print(f"   '{ticker}': {fund_id},")
    
    print(f"\n💾 Результаты сохранены в manual_mapping_results.json")

if __name__ == "__main__":
    main()