#!/usr/bin/env python3
"""
Парсер данных СЧА с сайтов управляющих компаний
Более надежный источник, чем investfunds.ru
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import json
from typing import Dict, List, Optional
import logging
from pathlib import Path

class ManagementCompanyParser:
    """Парсер данных с сайтов УК"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        self.logger = self._setup_logger()
        
        # Маппинг управляющих компаний на их сайты
        self.management_companies = {
            'Альфа-Капитал': {
                'base_url': 'https://alfabank.ru',
                'funds_url': 'https://alfabank.ru/make-money/pif/',
                'parser': self._parse_alfa_capital
            },
            'Сбербанк АМ': {
                'base_url': 'https://sberassetmanagement.ru',
                'funds_url': 'https://sberassetmanagement.ru/funds/',
                'parser': self._parse_sberbank
            },
            'Т-Капитал': {
                'base_url': 'https://t-capital.pro',
                'funds_url': 'https://t-capital.pro/funds/',
                'parser': self._parse_t_capital
            },
            'ВИМ': {
                'base_url': 'https://vim.capital',
                'funds_url': 'https://vim.capital/funds/',
                'parser': self._parse_vim
            }
        }
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('ManagementCompanyParser')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _parse_alfa_capital(self, ticker: str, isin: str) -> Optional[Dict]:
        """Парсит данные с сайта Альфа-Капитал"""
        
        try:
            # Альфа-Капитал публикует данные в формате отчетов
            funds_url = "https://alfabank.ru/make-money/pif/"
            
            response = self.session.get(funds_url, timeout=15)
            if response.status_code != 200:
                return None
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем таблицы с данными фондов
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                    
                    # Ищем по тикеру или ISIN
                    if ticker.upper() in row_text.upper() or isin in row_text:
                        # Пытаемся извлечь СЧА и цену пая
                        nav_match = re.search(r'(\d{1,3}(?:[\s,]\d{3})*(?:\.\d+)?)', row_text)
                        if nav_match:
                            nav_value = float(nav_match.group(1).replace(' ', '').replace(',', ''))
                            
                            return {
                                'ticker': ticker,
                                'nav': nav_value * 1000000,  # Конвертируем в рубли
                                'unit_price': 0,  # Нужно дополнительно искать
                                'source': 'Альфа-Капитал',
                                'url': funds_url
                            }
                            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга Альфа-Капитал для {ticker}: {e}")
        
        return None
    
    def _parse_sberbank(self, ticker: str, isin: str) -> Optional[Dict]:
        """Парсит данные с сайта Сбербанк АМ"""
        
        try:
            # Сбербанк АМ имеет API для получения данных
            api_url = f"https://sberassetmanagement.ru/api/funds/"
            
            response = self.session.get(api_url, timeout=15)
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Ищем фонд по тикеру или ISIN
                    for fund in data.get('funds', []):
                        if (fund.get('ticker', '').upper() == ticker.upper() or 
                            fund.get('isin') == isin):
                            
                            return {
                                'ticker': ticker,
                                'nav': fund.get('nav', 0),
                                'unit_price': fund.get('unit_price', 0),
                                'source': 'Сбербанк АМ',
                                'url': f"https://sberassetmanagement.ru/funds/{fund.get('id', '')}"
                            }
                            
                except json.JSONDecodeError:
                    pass
            
            # Fallback на парсинг HTML
            html_url = "https://sberassetmanagement.ru/funds/"
            response = self.session.get(html_url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем карточки фондов
                fund_cards = soup.find_all(['div', 'section'], class_=re.compile(r'fund|card'))
                
                for card in fund_cards:
                    card_text = card.get_text()
                    if ticker.upper() in card_text.upper():
                        # Ищем числовые значения
                        numbers = re.findall(r'(\d{1,3}(?:[\s,]\d{3})*(?:\.\d+)?)', card_text)
                        if numbers:
                            # Первое большое число - вероятно СЧА
                            for num_str in numbers:
                                try:
                                    num_val = float(num_str.replace(' ', '').replace(',', ''))
                                    if num_val > 1000000:  # Больше миллиона - похоже на СЧА
                                        return {
                                            'ticker': ticker,
                                            'nav': num_val,
                                            'unit_price': 0,
                                            'source': 'Сбербанк АМ',
                                            'url': html_url
                                        }
                                except ValueError:
                                    continue
                            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга Сбербанк АМ для {ticker}: {e}")
        
        return None
    
    def _parse_t_capital(self, ticker: str, isin: str) -> Optional[Dict]:
        """Парсит данные с сайта Т-Капитал"""
        
        try:
            funds_url = "https://t-capital.pro/funds/"
            
            response = self.session.get(funds_url, timeout=15)
            if response.status_code != 200:
                return None
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем ссылки на конкретные фонды
            fund_links = soup.find_all('a', href=re.compile(r'/funds/'))
            
            for link in fund_links:
                link_text = link.get_text()
                if ticker.upper() in link_text.upper():
                    
                    # Переходим на страницу конкретного фонда
                    fund_url = link.get('href')
                    if not fund_url.startswith('http'):
                        fund_url = "https://t-capital.pro" + fund_url
                    
                    fund_response = self.session.get(fund_url, timeout=15)
                    if fund_response.status_code == 200:
                        fund_soup = BeautifulSoup(fund_response.content, 'html.parser')
                        
                        # Ищем данные о СЧА и цене пая
                        fund_text = fund_soup.get_text()
                        
                        # Паттерны для поиска СЧА
                        nav_patterns = [
                            r'СЧА[:\s]*([0-9,.\s]+)',
                            r'чистых активов[:\s]*([0-9,.\s]+)',
                            r'активы[:\s]*([0-9,.\s]+)'
                        ]
                        
                        for pattern in nav_patterns:
                            match = re.search(pattern, fund_text, re.IGNORECASE)
                            if match:
                                nav_str = match.group(1).strip()
                                nav_value = self._parse_number(nav_str)
                                
                                if nav_value and nav_value > 1000000:
                                    return {
                                        'ticker': ticker,
                                        'nav': nav_value,
                                        'unit_price': 0,  # Дополнительно можно искать
                                        'source': 'Т-Капитал',
                                        'url': fund_url
                                    }
                    
                    time.sleep(1)
                    
        except Exception as e:
            self.logger.error(f"Ошибка парсинга Т-Капитал для {ticker}: {e}")
        
        return None
    
    def _parse_vim(self, ticker: str, isin: str) -> Optional[Dict]:
        """Парсит данные с сайта ВИМ (для LQDT)"""
        
        try:
            # ВИМ публикует отчеты по фондам
            funds_url = "https://vim.capital/funds/"
            
            response = self.session.get(funds_url, timeout=15)
            if response.status_code != 200:
                return None
                
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text()
            
            # Для LQDT ищем специфичные данные
            if ticker.upper() == 'LQDT' and 'LQDT' in page_text.upper():
                
                # Ищем актуальные данные о фонде
                nav_patterns = [
                    r'LQDT.*?(\d{1,3}(?:[\s,]\d{3})*(?:\.\d+)?)',
                    r'Ликвидность.*?(\d{1,3}(?:[\s,]\d{3})*(?:\.\d+)?)'
                ]
                
                for pattern in nav_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE | re.DOTALL)
                    for match in matches:
                        nav_value = self._parse_number(match)
                        if nav_value and nav_value > 100000000:  # Больше 100 млн
                            return {
                                'ticker': ticker,
                                'nav': nav_value,
                                'unit_price': 0,
                                'source': 'ВИМ',
                                'url': funds_url
                            }
                            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга ВИМ для {ticker}: {e}")
        
        return None
    
    def _parse_number(self, text: str) -> Optional[float]:
        """Парсит число из текста"""
        
        if not text:
            return None
        
        try:
            # Убираем все кроме цифр, точек и запятых
            cleaned = re.sub(r'[^\d.,]', '', str(text).strip())
            
            if not cleaned:
                return None
            
            # Определяем разделители
            if ',' in cleaned and '.' in cleaned:
                if cleaned.rfind(',') > cleaned.rfind('.'):
                    cleaned = cleaned.replace('.', '').replace(',', '.')
                else:
                    cleaned = cleaned.replace(',', '')
            elif ',' in cleaned:
                if cleaned.count(',') == 1 and len(cleaned.split(',')[1]) <= 2:
                    cleaned = cleaned.replace(',', '.')
                else:
                    cleaned = cleaned.replace(',', '')
            
            return float(cleaned)
            
        except (ValueError, AttributeError):
            return None
    
    def get_fund_data_by_management_company(self, ticker: str, isin: str, management_company: str) -> Optional[Dict]:
        """Получает данные фонда с сайта УК"""
        
        # Нормализуем название УК
        uc_mappings = {
            'Альфа-Капитал': ['Альфа-Капитал', 'A-Capital', 'АЛЬФА'],
            'Сбербанк АМ': ['Сбербанк АМ', 'Sberbank AM', 'СБЕР'],
            'Т-Капитал': ['Т-Капитал', 'T-Capital', 'ТИНЬКОФФ'],
            'ВИМ': ['ВИМ', 'VIM', 'ВИМ Инвестиции']
        }
        
        matched_uc = None
        for uc_name, aliases in uc_mappings.items():
            if any(alias.upper() in management_company.upper() for alias in aliases):
                matched_uc = uc_name
                break
        
        if not matched_uc or matched_uc not in self.management_companies:
            return None
        
        parser_func = self.management_companies[matched_uc]['parser']
        
        try:
            return parser_func(ticker, isin)
        except Exception as e:
            self.logger.error(f"Ошибка парсинга {matched_uc} для {ticker}: {e}")
            return None
    
    def batch_parse_funds(self, etf_data: pd.DataFrame) -> Dict[str, Dict]:
        """Массовый парсинг фондов с сайтов УК"""
        
        results = {}
        
        # Группируем фонды по УК для оптимизации
        uc_groups = etf_data.groupby('management_company')
        
        for uc_name, group in uc_groups:
            self.logger.info(f"🏢 Обрабатываем УК: {uc_name} ({len(group)} фондов)")
            
            for _, row in group.iterrows():
                ticker = row['ticker']
                isin = row.get('isin', '')
                
                self.logger.info(f"  🔍 Ищем {ticker}")
                
                fund_data = self.get_fund_data_by_management_company(ticker, isin, uc_name)
                
                if fund_data:
                    results[ticker] = fund_data
                    self.logger.info(f"  ✅ {ticker}: СЧА {fund_data['nav']/1e9:.1f} млрд ₽")
                else:
                    self.logger.info(f"  ❌ {ticker}: не найден")
                
                time.sleep(1)  # Пауза между запросами
        
        return results

def main():
    """Тестирование"""
    
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
    
    # Создаем парсер
    parser = ManagementCompanyParser()
    
    # Тестируем на небольшой выборке
    test_funds = etf_data[etf_data['ticker'].isin(['LQDT', 'AKMM', 'SBMM', 'TPAY'])].copy()
    
    print(f"🧪 Тестируем на {len(test_funds)} фондах")
    
    results = parser.batch_parse_funds(test_funds)
    
    print(f"\n📊 Результаты:")
    for ticker, data in results.items():
        print(f"  ✅ {ticker}: {data['nav']/1e9:.1f} млрд ₽ (источник: {data['source']})")
    
    print(f"\n🎯 Покрытие: {len(results)}/{len(test_funds)} = {len(results)/len(test_funds)*100:.1f}%")

if __name__ == "__main__":
    main()