#!/usr/bin/env python3
"""
Поиск фондов по ISIN кодам на различных источниках
Более надежная альтернатива поиску по названиям
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

class ISINBasedFundFinder:
    """Поиск фондов по ISIN кодам"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        self.logger = self._setup_logger()
        
        # Различные источники для поиска по ISIN
        self.sources = {
            'investfunds': 'https://investfunds.ru',
            'cbr': 'https://www.cbr.ru',
            'moex': 'https://www.moex.com',
            'finam': 'https://www.finam.ru',
        }
        
        self.found_mappings = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('ISINFundFinder')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def search_by_isin_investfunds(self, isin: str, ticker: str) -> Optional[Dict]:
        """Поиск по ISIN на investfunds.ru"""
        
        try:
            # Попробуем несколько способов поиска
            search_urls = [
                f"https://investfunds.ru/search/?q={isin}",
                f"https://investfunds.ru/funds/?search={isin}",
                f"https://investfunds.ru/funds/?isin={isin}"
            ]
            
            for search_url in search_urls:
                self.logger.info(f"Ищем {ticker} ({isin}) по URL: {search_url}")
                
                response = self.session.get(search_url, timeout=15)
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем ссылки на фонды
                fund_links = soup.find_all('a', href=re.compile(r'/funds/\d+/'))
                
                for link in fund_links:
                    href = link.get('href', '')
                    fund_id_match = re.search(r'/funds/(\d+)/', href)
                    
                    if fund_id_match:
                        fund_id = int(fund_id_match.group(1))
                        link_text = link.get_text(strip=True)
                        
                        # Проверяем, содержит ли страница ISIN или тикер
                        page_text = soup.get_text().upper()
                        if isin in page_text or ticker.upper() in page_text:
                            
                            self.logger.info(f"✅ Найден {ticker}: ID {fund_id} на investfunds.ru")
                            return {
                                'source': 'investfunds.ru',
                                'fund_id': fund_id,
                                'url': f"https://investfunds.ru{href}",
                                'name': link_text,
                                'confidence': 0.9
                            }
                
                time.sleep(1)  # Пауза между запросами
                
        except Exception as e:
            self.logger.error(f"Ошибка поиска {ticker} на investfunds.ru: {e}")
        
        return None
    
    def search_by_isin_cbr(self, isin: str, ticker: str) -> Optional[Dict]:
        """Поиск по ISIN на сайте ЦБ РФ"""
        
        try:
            # ЦБ РФ ведет реестр всех ПИФов
            search_url = f"https://www.cbr.ru/registries/?UniDbQuery.Posted=True&UniDbQuery.SearchText={isin}"
            
            self.logger.info(f"Ищем {ticker} ({isin}) в реестре ЦБ РФ")
            
            response = self.session.get(search_url, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем таблицы с результатами
                tables = soup.find_all('table')
                
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                        
                        if isin in row_text or ticker.upper() in row_text.upper():
                            self.logger.info(f"✅ Найден {ticker} в реестре ЦБ РФ")
                            return {
                                'source': 'cbr.ru',
                                'url': search_url,
                                'name': row_text[:100],
                                'confidence': 0.95,
                                'official': True
                            }
                
        except Exception as e:
            self.logger.error(f"Ошибка поиска {ticker} в ЦБ РФ: {e}")
        
        return None
    
    def search_by_isin_moex(self, isin: str, ticker: str) -> Optional[Dict]:
        """Поиск дополнительных данных по ISIN на MOEX"""
        
        try:
            # MOEX API для поиска по ISIN
            search_url = f"https://iss.moex.com/iss/securities.json?q={isin}"
            
            response = self.session.get(search_url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                
                securities = data.get('securities', {}).get('data', [])
                columns = data.get('securities', {}).get('columns', [])
                
                for security in securities:
                    if len(security) >= len(columns):
                        security_dict = dict(zip(columns, security))
                        
                        if security_dict.get('isin') == isin:
                            self.logger.info(f"✅ Найден {ticker} в MOEX с подробными данными")
                            return {
                                'source': 'moex.com',
                                'secid': security_dict.get('secid'),
                                'shortname': security_dict.get('shortname'),
                                'name': security_dict.get('name'),
                                'isin': security_dict.get('isin'),
                                'confidence': 1.0,
                                'official': True
                            }
                
        except Exception as e:
            self.logger.error(f"Ошибка поиска {ticker} на MOEX: {e}")
        
        return None
    
    def get_nav_data_from_source(self, ticker: str, source_info: Dict) -> Optional[Dict]:
        """Получает данные СЧА из найденного источника"""
        
        if source_info['source'] == 'investfunds.ru' and 'fund_id' in source_info:
            try:
                from investfunds_parser import InvestFundsParser
                parser = InvestFundsParser()
                
                fund_data = parser.get_fund_data(source_info['fund_id'])
                if fund_data and fund_data.get('nav', 0) > 0:
                    return {
                        'ticker': ticker,
                        'nav': fund_data['nav'],
                        'unit_price': fund_data['unit_price'],
                        'name': fund_data['name'],
                        'source': 'investfunds.ru',
                        'fund_id': source_info['fund_id'],
                        'date': fund_data['date']
                    }
                    
            except Exception as e:
                self.logger.error(f"Ошибка получения данных из investfunds.ru для {ticker}: {e}")
        
        return None
    
    def find_all_funds_by_isin(self, etf_data: pd.DataFrame, max_funds: int = None) -> Dict[str, Dict]:
        """Ищет все фонды по их ISIN кодам"""
        
        self.logger.info(f"🔍 Начинаем поиск по ISIN для {len(etf_data)} фондов")
        
        results = {}
        found_count = 0
        
        for idx, row in etf_data.iterrows():
            if max_funds and found_count >= max_funds:
                break
                
            ticker = row['ticker']
            isin = row.get('isin', '')
            
            if not isin or pd.isna(isin):
                self.logger.warning(f"❌ {ticker}: нет ISIN кода")
                continue
            
            self.logger.info(f"\n🔍 Ищем {ticker} (ISIN: {isin})")
            
            # Пробуем разные источники
            source_results = []
            
            # 1. investfunds.ru
            investfunds_result = self.search_by_isin_investfunds(isin, ticker)
            if investfunds_result:
                source_results.append(investfunds_result)
            
            # 2. ЦБ РФ
            cbr_result = self.search_by_isin_cbr(isin, ticker)
            if cbr_result:
                source_results.append(cbr_result)
            
            # 3. MOEX
            moex_result = self.search_by_isin_moex(isin, ticker)
            if moex_result:
                source_results.append(moex_result)
            
            if source_results:
                # Выбираем лучший источник (приоритет у investfunds.ru для получения СЧА)
                best_source = None
                for result in source_results:
                    if result['source'] == 'investfunds.ru':
                        best_source = result
                        break
                
                if not best_source:
                    best_source = max(source_results, key=lambda x: x.get('confidence', 0))
                
                # Пытаемся получить данные СЧА
                nav_data = self.get_nav_data_from_source(ticker, best_source)
                
                if nav_data:
                    results[ticker] = nav_data
                    found_count += 1
                    self.logger.info(f"✅ {ticker}: СЧА {nav_data['nav']/1e9:.1f} млрд ₽")
                else:
                    results[ticker] = {
                        'ticker': ticker,
                        'source_info': best_source,
                        'status': 'found_but_no_nav'
                    }
                    self.logger.info(f"⚠️  {ticker}: найден, но нет данных СЧА")
            else:
                self.logger.warning(f"❌ {ticker}: не найден ни в одном источнике")
            
            # Пауза между поисками
            time.sleep(2)
        
        self.logger.info(f"\n📊 Итоги поиска по ISIN:")
        self.logger.info(f"   ✅ Найдено с данными СЧА: {found_count}")
        self.logger.info(f"   📋 Всего обработано: {len(results)}")
        
        return results
    
    def save_results(self, results: Dict, filename: str = "isin_search_results.json"):
        """Сохраняет результаты поиска"""
        
        # Подготавливаем данные для сохранения
        save_data = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'total_searched': len(results),
            'successful_nav_extraction': len([r for r in results.values() if 'nav' in r]),
            'results': results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"💾 Результаты сохранены в {filename}")
    
    def create_updated_mapping(self, results: Dict) -> Dict[str, int]:
        """Создает обновленный маппинг для investfunds_parser.py"""
        
        mapping = {}
        
        for ticker, data in results.items():
            if 'fund_id' in data and 'nav' in data:
                mapping[ticker] = data['fund_id']
        
        self.logger.info(f"📋 Создан маппинг для {len(mapping)} фондов")
        
        return mapping

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
    print(f"📊 Загружаем данные из {latest_data}")
    
    etf_data = pd.read_csv(latest_data)
    print(f"✅ Загружено {len(etf_data)} ETF с ISIN кодами")
    
    # Создаем поисковик
    finder = ISINBasedFundFinder()
    
    # Запускаем поиск (ограничиваем первыми 20 для теста)
    print(f"\n🚀 Запускаем поиск по ISIN (тестируем первые 20 фондов)")
    
    results = finder.find_all_funds_by_isin(etf_data.head(20), max_funds=20)
    
    # Сохраняем результаты
    finder.save_results(results)
    
    # Создаем обновленный маппинг
    new_mapping = finder.create_updated_mapping(results)
    
    if new_mapping:
        print(f"\n✅ Новый маппинг для investfunds_parser.py:")
        for ticker, fund_id in new_mapping.items():
            print(f"   '{ticker}': {fund_id},")
    
    print(f"\n🎉 Поиск по ISIN завершен!")
    print(f"📄 Результаты сохранены в isin_search_results.json")

if __name__ == "__main__":
    main()