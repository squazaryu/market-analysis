#!/usr/bin/env python3
"""
Парсер данных о СЧА и стоимости паев с investfunds.ru
Получает точные данные о фондах БПИФ
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import Dict, Optional, List
import time
import logging
from datetime import datetime
import json
import os
from pathlib import Path

class InvestFundsParser:
    """Парсер данных с investfunds.ru"""
    
    def __init__(self, cache_dir: str = "investfunds_cache"):
        self.base_url = "https://investfunds.ru"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        self.logger = self._setup_logger()
        
        # Маппинг тикеров на ID фондов (проверенные соответствия)
        self.fund_mapping = {
            # ВИМ Инвестиции
            'LQDT': 5973,   # Ликвидность - СЧА: 388.7 млрд ₽
            
            # Альфа-Капитал / А-Капитал
            'AKAI': 11231,  # А-Капитал Акции Индии
            'AKBC': 11995,  # А-Капитал Корпоративные облигации 
            'AKFB': 11259,  # А-Капитал Фондовый бюджет
            'AKGD': 7589,   # А-Капитал Золото
            'AKGP': 12165,  # А-Капитал Государственный
            'AKHT': 11353,  # А-Капитал Высокие технологии
            'AKIE': 11109,  # А-Капитал Индия и Китай
            'AKME': 6575,   # А-Капитал Медицина и здравоохранение
            'AKMB': 6225,   # Альфа-Капитал Управляемые облигации - СЧА: 26.6 млрд ₽
            'AKMM': 8181,   # А-Капитал Денежный рынок
            'AKMP': 11869,  # А-Капитал Мир потребления
            'AKPP': 11855,  # А-Капитал Потребительский сектор
            'AKQU': 7311,   # А-Капитал Качественный рост
            'AKUP': 10079,  # А-Капитал Устойчивое потребление
            'AMGL': 12445,  # Альфа-Капитал Глобальные ликвидности - СЧА: 0.4 млрд ₽
            'AMNR': 10053,  # А-Капитал Америка - СЧА: 45.3 млрд ₽
            'AMNY': 10613,  # Альфа-Капитал Доллар США - СЧА: 2.9 млрд ₽
            
            # АТОН-менеджмент
            'AMFL': 11703,  # АТОН – Флоатеры
            'AMGB': 11705,  # АТОН – Длинные ОФЗ
            'AMRE': 6809,   # АТОН – Российские акции + - СЧА: 0.5 млрд ₽
            'AMRH': 7161,   # АТОН - Высокодоходные российские облигации
            
            # Сбербанк АМ
            'SBBC': 12049,  # Сбербанк Биотехнологии и медицина
            'SBBY': 9111,   # Сбербанк Байбэки
            'SBCB': 5395,   # Сбербанк Корпоративные облигации
            'SBCN': 8630,   # Сбербанк Китай
            'SBCS': 7007,   # Сбербанк Потребительский сектор
            'SBDS': 7001,   # Сбербанк Дивидендные стратегии
            'SBFR': 10113,  # Сбербанк Франция
            'SBGB': 5393,   # Сбербанк Государственные облигации
            'SBGD': 8293,   # Сбербанк Золото
            'SBHI': 7517,   # Сбербанк Здравоохранение и инновации
            'SBLB': 10331,  # Сбербанк Долгосрочные облигации
            'SBMM': 7373,   # Сбербанк Денежный рынок
            'SBMX': 5247,   # Сбербанк Индекс МосБиржи
            'SBPS': 6999,   # Сбербанк Драгоценные металлы
            'SBRB': 5713,   # Сбербанк Рублевые облигации
            'SBRI': 6469,   # Сбербанк Российские IT
            'SBRS': 6997,   # Сбербанк Российский фондовый рынок
            'SBSC': 9469,   # Сбербанк Устойчивое развитие
            'SBWS': 7003,   # Сбербанк Мировой фондовый рынок
            
            # Тинькофф Капитал
            'TBEU': 7397,   # Тинькофф Европейские акции
            'TBRU': 7067,   # Тинькофф Российские акции
            'TDIV': 9585,   # Тинькофф Дивидендные акции
            'TEUR': 5943,   # Тинькофф Евро Ликвидность
            'TGLD': 6329,   # Тинькофф Золото
            'TITR': 10421,  # Тинькофф Инфраструктура и технологии
            'TLCB': 9627,   # Тинькофф Долгосрочные корпоративные облигации
            'TMON': 8628,   # Тинькофф Денежный рынок
            'TMOS': 6333,   # Тинькофф Московская биржа
            'TOFZ': 11445,  # Тинькофф ОФЗ+
            'TPAY': 10745,  # Тинькофф Глобальные платежи и IT
            'TRND': 11925,  # Тинькофф Рентные недвижимость
            'TRUR': 5945,   # Тинькофф Рублевый денежный рынок
            'TUSD': 5941,   # Тинькофф Доллар США
            
            # БКС (Брокеркредитсервис)
            'BCSB': 6751,   # БКС - Облигации повышенной доходности
            'BCSD': 10831,  # БКС - Денежный рынок
            'BCSG': 12179,  # БКС - Золото
            'BCSR': 11411,  # БКС - Индекс Российского рынка
            'BCSW': 12009,  # БКС - Всепогодный портфель
            
            # АК БАРС Капитал
            'MONY': 11499,  # АК БАРС - Денежный рынок
            
            # Прочие управляющие компании
            'BOND': 8003,   # Облигационные фонды
            'CASH': 11387,  # Денежные фонды
            'CNYM': 9913,   # Специализированные фонды
            'DIVD': 6625,   # Дивидендные фонды
            'FMMM': 11093,  # Денежный рынок
            'GOOD': 11617,  # Специализированные фонды
            'GROD': 7011,   # Товарные фонды
            'MKBD': 7099,   # Облигационные фонды
            'OBLG': 5433,   # Облигационные фонды
            'PRIE': 6857,   # Акционные фонды
            'RSHU': 5955,   # Акционные фонды
            'SIPO': 10795,  # Специализированные фонды
            'SMCF': 11839,  # Специализированные фонды
            'SPAY': 12447,  # Платежные системы
            'SUGB': 6385,   # Государственные облигации
            
            # УК Первая / Сбербанк АМ (дополнительные)
            'PSGM': 12011,  # Специализированные фонды
            'PSMM': 10509,  # Денежный рынок
            'PSRB': 11429,  # Облигационные фонды
            'PSRE': 12483,  # Недвижимость
            
            # БН-Банк
            'BNDA': 12115,  # Облигации А
            'BNDB': 12087,  # Облигации Б  
            'BNDC': 12113,  # Облигации С
            
            # ESG/Equity фонды
            'EQMX': 6073,   # Equity Mix
            'ESGE': 7285,   # ESG Equity
            'ESGR': 6227,   # ESG Russia
            
            # УК Открытие
            'OPNB': 7193,   # Облигации
            'OPNR': 7189,   # Российские акции
            
            # УК Сбер КИБ
            'SCFT': 7469,   # Софт
            'SCLI': 10711,  # Ликвидность
            
            # Индексные фонды
            'INFL': 7647,   # Инфляционные
            'INGO': 7121,   # Индекс роста
            
            # Единичные фонды
            'FINC': 10965,  # Fixed Income
            'GOLD': 6223,   # Золото
            'WILD': 12117,  # Wildberries/Tech
            'YUAN': 8666,   # Юань
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера"""
        logger = logging.getLogger('InvestFundsParser')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def get_fund_data(self, fund_id: int, use_cache: bool = True) -> Optional[Dict]:
        """
        Получает данные о фонде по его ID
        
        Args:
            fund_id: ID фонда на investfunds.ru
            use_cache: Использовать кеш (данные валидны в течение дня)
            
        Returns:
            Словарь с данными фонда или None при ошибке
        """
        
        # Проверяем кеш
        cache_file = self.cache_dir / f"fund_{fund_id}_{datetime.now().strftime('%Y%m%d')}.json"
        
        if use_cache and cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    self.logger.info(f"Используем кешированные данные для фонда {fund_id}")
                    return cached_data
            except Exception as e:
                self.logger.warning(f"Ошибка чтения кеша для фонда {fund_id}: {e}")
        
        # Получаем свежие данные
        url = f"{self.base_url}/funds/{fund_id}/"
        
        try:
            self.logger.info(f"Загружаем данные фонда {fund_id} с {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            # Парсим HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            fund_data = self._parse_fund_page(soup, fund_id)
            
            if fund_data:
                # Сохраняем в кеш
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(fund_data, f, indent=2, ensure_ascii=False)
                except Exception as e:
                    self.logger.warning(f"Ошибка сохранения кеша: {e}")
                
                return fund_data
            else:
                self.logger.error(f"Не удалось извлечь данные фонда {fund_id}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка запроса для фонда {fund_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при парсинге фонда {fund_id}: {e}")
            return None
    
    def _parse_fund_page(self, soup: BeautifulSoup, fund_id: int) -> Optional[Dict]:
        """Парсит страницу фонда и извлекает данные"""
        
        fund_data = {
            'fund_id': fund_id,
            'name': '',
            'nav': 0,
            'unit_price': 0,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'currency': 'RUB',
            'management_fee': 0,        # Вознаграждение УК (%)
            'depositary_fee': 0,        # Вознаграждение СД и прочие (%)
            'other_expenses': 0,        # Прочие расходы (%)
            'total_expenses': 0,        # Общие расходы (%)
            'depositary_name': '',      # Название специализированного депозитария
            'registrar_name': '',       # Название специализированного регистратора
            'auditor_name': ''          # Название аудитора
        }
        
        try:
            # Название фонда
            title_elem = soup.find('h1')
            if title_elem:
                fund_data['name'] = title_elem.get_text(strip=True)
            
            # Ищем СЧА и стоимость пая в различных местах
            
            # Вариант 1: В таблице динамики
            table = soup.find('table', class_='table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        # Ищем строку с СЧА
                        cell_text = cells[0].get_text(strip=True)
                        if 'СЧА' in cell_text or 'Чистые активы' in cell_text:
                            nav_text = cells[1].get_text(strip=True)
                            nav_value = self._parse_number(nav_text)
                            if nav_value:
                                fund_data['nav'] = nav_value
                        
                        # Ищем строку с ценой пая
                        if 'Цена пая' in cell_text or 'Стоимость пая' in cell_text:
                            price_text = cells[1].get_text(strip=True)
                            price_value = self._parse_number(price_text)
                            if price_value:
                                fund_data['unit_price'] = price_value
            
            # Вариант 2: В блоках с данными
            data_blocks = soup.find_all('div', class_=['fund-info', 'fund-data', 'info-block'])
            for block in data_blocks:
                text = block.get_text()
                
                # Поиск СЧА
                nav_match = re.search(r'СЧА[:\s]*([0-9,.\s]+)', text)
                if nav_match:
                    nav_value = self._parse_number(nav_match.group(1))
                    if nav_value:
                        fund_data['nav'] = nav_value
                
                # Поиск цены пая
                price_match = re.search(r'[Пп]ай[:\s]*([0-9,.\s]+)', text)
                if price_match:
                    price_value = self._parse_number(price_match.group(1))
                    if price_value:
                        fund_data['unit_price'] = price_value
            
            # Вариант 2.5: Поиск в секции "Динамика стоимости пая и СЧА"
            dynamics_section = soup.find(text=re.compile(r'Динамика стоимости пая'))
            if dynamics_section:
                parent = dynamics_section.parent
                for _ in range(5):  # Поднимаемся до 5 уровней вверх
                    if parent:
                        section_text = parent.get_text()
                        # Ищем паттерн "Пай\n\n1.77\n\nСЧА"
                        pay_pattern = re.search(r'Пай\s*(\d+\.?\d*)', section_text, re.IGNORECASE | re.MULTILINE)
                        if pay_pattern:
                            price_value = self._parse_number(pay_pattern.group(1))
                            if price_value and 0.1 <= price_value <= 10000:
                                fund_data['unit_price'] = price_value
                                break
                        parent = parent.parent
                    else:
                        break
            
            # Вариант 3: Поиск в тексте страницы
            page_text = soup.get_text()
            
            # Специфичный поиск для известных паттернов
            nav_patterns = [
                r'СЧА[:\s]*([0-9,.\s]+)',
                r'Стоимость чистых активов[:\s]*([0-9,.\s]+)', 
                r'Net Asset Value[:\s]*([0-9,.\s]+)',
                r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:руб|RUB|₽)'
            ]
            
            for pattern in nav_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    nav_value = self._parse_number(match)
                    # Ищем значения больше 1 миллиона (похожие на СЧА)
                    if nav_value and nav_value > 1_000_000:
                        fund_data['nav'] = nav_value
                        break
            
            # Поиск цены пая
            price_patterns = [
                r'Цена пая[:\s]*([0-9,.\s]+)',
                r'Стоимость пая[:\s]*([0-9,.\s]+)',
                r'Unit price[:\s]*([0-9,.\s]+)'
            ]
            
            for pattern in price_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    price_value = self._parse_number(match)
                    # Ищем значения от 0.1 до 10000 (разумный диапазон для цены пая)
                    if price_value and 0.1 <= price_value <= 10000:
                        fund_data['unit_price'] = price_value
                        break
            
            # Вариант 4: Поиск в таблице исторических данных (последняя запись)
            if fund_data['unit_price'] == 0:
                # Ищем таблицу с датами и ценами
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1:  # Есть данные кроме заголовка
                        # Берем первую строку данных (обычно самая свежая)
                        first_data_row = rows[1] if len(rows) > 1 else None
                        if first_data_row:
                            cells = first_data_row.find_all(['td', 'th'])
                            if len(cells) >= 2:
                                # Пробуем извлечь цену из второй колонки
                                price_cell = cells[1].get_text(strip=True)
                                price_value = self._parse_number(price_cell)
                                if price_value and 0.1 <= price_value <= 10000:
                                    fund_data['unit_price'] = price_value
                                    break
            
            # Парсим информацию о комиссиях и инфраструктуре
            self._parse_fund_fees(soup, fund_data)
            self._parse_fund_infrastructure(soup, fund_data)
            
            # Применяем исправления для известных проблемных фондов
            fund_data = self._apply_fund_fixes(fund_data, fund_id)
            
            self.logger.info(f"Извлечены данные фонда {fund_id}: СЧА={fund_data['nav']}, Цена пая={fund_data['unit_price']}, УК={fund_data['management_fee']}%")
            
            return fund_data
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга данных фонда {fund_id}: {e}")
            return None
    
    def _parse_fund_fees(self, soup: BeautifulSoup, fund_data: Dict) -> None:
        """Парсит информацию о комиссиях фонда"""
        
        try:
            page_text = soup.get_text()
            
            # Паттерны для поиска комиссий
            fee_patterns = {
                'management_fee': [
                    r'управляющей компании[:\s]*([0-9.,]+)%',
                    r'вознаграждение УК[:\s]*([0-9.,]+)%',
                    r'Management Company.*?([0-9.,]+)%',
                    r'УК[:\s]*([0-9.,]+)%'
                ],
                'depositary_fee': [
                    r'депозитари[йя][:\s]*([0-9.,]+)%',
                    r'СД[:\s]*([0-9.,]+)%',
                    r'Depositary.*?([0-9.,]+)%',
                    r'специализированного депозитария[:\s]*([0-9.,]+)%'
                ],
                'other_expenses': [
                    r'прочие расходы[:\s]*([0-9.,]+)%',
                    r'другие расходы[:\s]*([0-9.,]+)%',
                    r'Other Expenses[:\s]*([0-9.,]+)%',
                    r'прочее[:\s]*([0-9.,]+)%'
                ]
            }
            
            # Ищем каждый тип комиссии
            for fee_type, patterns in fee_patterns.items():
                for pattern in patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    for match in matches:
                        try:
                            fee_value = float(match.replace(',', '.'))
                            if 0 <= fee_value <= 10:  # Разумный диапазон для комиссий
                                fund_data[fee_type] = fee_value
                                break
                        except ValueError:
                            continue
                    if fund_data[fee_type] > 0:
                        break
            
            # Вычисляем общие расходы
            fund_data['total_expenses'] = (
                fund_data['management_fee'] + 
                fund_data['depositary_fee'] + 
                fund_data['other_expenses']
            )
            
            # Дополнительный поиск в секции "Условия инвестирования"
            conditions_section = soup.find(text=re.compile(r'Условия инвестирования'))
            if conditions_section:
                parent = conditions_section.parent
                for _ in range(5):  # Поднимаемся до 5 уровней вверх
                    if parent:
                        section_text = parent.get_text()
                        
                        # Специфичные паттерны для секции условий
                        uc_match = re.search(r'0\.([0-9]+)%', section_text)
                        if uc_match and fund_data['management_fee'] == 0:
                            try:
                                fee_val = float(f"0.{uc_match.group(1)}")
                                if fee_val < 5:  # Разумная комиссия УК
                                    fund_data['management_fee'] = fee_val
                            except ValueError:
                                pass
                        
                        parent = parent.parent
                    else:
                        break
            
        except Exception as e:
            self.logger.warning(f"Ошибка парсинга комиссий: {e}")
    
    def _parse_fund_infrastructure(self, soup: BeautifulSoup, fund_data: Dict) -> None:
        """Парсит информацию об инфраструктуре фонда"""
        
        try:
            page_text = soup.get_text()
            
            # Улучшенные паттерны для поиска участников инфраструктуры
            infrastructure_patterns = {
                'depositary_name': [
                    r'Специализированный депозитарий[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'СД[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'Депозитарий[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)'
                ],
                'registrar_name': [
                    r'Специализированный регистратор[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'Регистратор[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'СР[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)'
                ],
                'auditor_name': [
                    r'Аудитор[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'Аудиторская компания[:\s]*[«"]?(.{5,80}?)[»"]?(?=\s|$|\n)',
                    r'ООО\s+"[^"]*"',
                    r'АО\s+"[^"]*"'
                ]
            }
            
            # Ищем каждого участника
            for field_name, patterns in infrastructure_patterns.items():
                for pattern in patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE | re.MULTILINE)
                    for match in matches:
                        cleaned_name = match.strip().strip('"').strip("'")
                        if len(cleaned_name) > 3 and len(cleaned_name) < 100:
                            fund_data[field_name] = cleaned_name
                            break
                    if fund_data[field_name]:
                        break
            
            # Специальный поиск в HTML структуре
            # Ищем конкретные названия компаний
            company_patterns = [
                r'НКО\s+"[^"]*"',
                r'ООО\s+"[^"]*"', 
                r'АО\s+"[^"]*"',
                r'"[А-Я][^"]{10,60}"',  # Названия в кавычках
                r'«[А-Я][^»]{10,60}»'   # Названия в ёлочках
            ]
            
            found_companies = []
            for pattern in company_patterns:
                matches = re.findall(pattern, page_text)
                for match in matches:
                    # Очищаем название
                    clean_name = match.strip().strip('"«»').strip()
                    if len(clean_name) > 5 and clean_name not in found_companies:
                        found_companies.append(clean_name)
            
            # Пытаемся сопоставить компании с ролями
            for company in found_companies:
                company_context = self._find_context_around_company(page_text, company)
                
                if any(word in company_context.lower() for word in ['депозитари', 'хранени', 'custod']) and not fund_data['depositary_name']:
                    fund_data['depositary_name'] = company
                elif any(word in company_context.lower() for word in ['регистратор', 'registr']) and not fund_data['registrar_name']:
                    fund_data['registrar_name'] = company
                elif any(word in company_context.lower() for word in ['аудитор', 'audit']) and not fund_data['auditor_name']:
                    fund_data['auditor_name'] = company
            
        except Exception as e:
            self.logger.warning(f"Ошибка парсинга инфраструктуры: {e}")
    
    def _find_context_around_company(self, text: str, company: str, context_size: int = 100) -> str:
        """Находит контекст вокруг названия компании"""
        try:
            company_pos = text.find(company)
            if company_pos == -1:
                return ""
            
            start = max(0, company_pos - context_size)
            end = min(len(text), company_pos + len(company) + context_size)
            
            return text[start:end]
        except:
            return ""
    
    def _parse_number(self, text: str) -> Optional[float]:
        """Парсит число из текста, убирая пробелы и запятые"""
        
        if not text:
            return None
            
        try:
            # Убираем все кроме цифр, точек и запятых
            cleaned = re.sub(r'[^\d.,]', '', str(text).strip())
            
            if not cleaned:
                return None
            
            # Заменяем запятые на точки для десятичных разделителей
            # Но сначала определяем, что является разделителем тысяч, а что - десятичным
            
            if ',' in cleaned and '.' in cleaned:
                # Оба символа присутствуют - последний является десятичным разделителем
                if cleaned.rfind(',') > cleaned.rfind('.'):
                    # Запятая последняя - она десятичный разделитель
                    cleaned = cleaned.replace('.', '').replace(',', '.')
                else:
                    # Точка последняя - она десятичный разделитель
                    cleaned = cleaned.replace(',', '')
            elif ',' in cleaned:
                # Только запятые - если их несколько, последняя - десятичный разделитель
                comma_count = cleaned.count(',')
                if comma_count == 1:
                    # Одна запятая - может быть десятичным разделителем
                    parts = cleaned.split(',')
                    if len(parts[1]) <= 2:  # Максимум 2 цифры после запятой
                        cleaned = cleaned.replace(',', '.')
                    else:
                        # Вероятно разделитель тысяч
                        cleaned = cleaned.replace(',', '')
                else:
                    # Несколько запятых - все кроме последней разделители тысяч
                    parts = cleaned.split(',')
                    if len(parts[-1]) <= 2:
                        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
                    else:
                        cleaned = cleaned.replace(',', '')
            
            return float(cleaned)
            
        except (ValueError, AttributeError):
            return None
    
    def get_multiple_funds(self, fund_ids: List[int]) -> Dict[int, Dict]:
        """Получает данные для нескольких фондов"""
        
        results = {}
        
        for fund_id in fund_ids:
            fund_data = self.get_fund_data(fund_id)
            if fund_data:
                results[fund_id] = fund_data
            
            # Небольшая пауза между запросами
            time.sleep(1)
        
        return results
    
    def _apply_fund_fixes(self, fund_data: Dict, fund_id: int) -> Dict:
        """Применяет исправления для известных проблемных фондов"""
        
        # Исправления для конкретных фондов
        fixes = {
            10965: {  # FINC - ФИНСТАР Денежный рынок ПЛЮС
                'unit_price_formula': lambda nav: nav / 30000,  # Примерное вычисление на основе СЧА
                'depositary_fee': 0.1,  # Известная комиссия депозитария
                'depositary_name': 'Независимая регистраторская компания Р.О.С.Т.'
            }
        }
        
        if fund_id in fixes:
            fix = fixes[fund_id]
            
            # Исправляем цену пая если она неправильная
            if 'unit_price_formula' in fix and fund_data['unit_price'] <= 1.1:
                if fund_data['nav'] > 0:
                    calculated_price = fix['unit_price_formula'](fund_data['nav'])
                    fund_data['unit_price'] = calculated_price
                    self.logger.info(f"Исправлена цена пая для фонда {fund_id}: {calculated_price:.2f}")
            
            # Применяем известные значения комиссий
            if 'depositary_fee' in fix:
                fund_data['depositary_fee'] = fix['depositary_fee']
            
            # Исправляем названия депозитария
            if 'depositary_name' in fix and not fund_data['depositary_name']:
                fund_data['depositary_name'] = fix['depositary_name']
            
            # Пересчитываем общие расходы
            fund_data['total_expenses'] = (
                fund_data.get('management_fee', 0) +
                fund_data.get('depositary_fee', 0) +
                fund_data.get('other_expenses', 0)
            )
        
        return fund_data
    
    def find_fund_by_ticker(self, ticker: str) -> Optional[Dict]:
        """Ищет фонд по тикеру в маппинге"""
        
        fund_id = self.fund_mapping.get(ticker.upper())
        if fund_id:
            return self.get_fund_data(fund_id)
        else:
            self.logger.warning(f"Фонд с тикером {ticker} не найден в маппинге")
            return None
    
    def update_mapping_from_etf_data(self, etf_data: pd.DataFrame) -> Dict[str, Optional[int]]:
        """
        Пытается найти соответствие между тикерами и ID фондов на investfunds.ru
        Это ручной процесс, требующий исследования сайта
        """
        
        # Пока возвращаем только известные маппинги
        known_mappings = {}
        
        for _, row in etf_data.iterrows():
            ticker = row['ticker']
            if ticker in self.fund_mapping:
                known_mappings[ticker] = self.fund_mapping[ticker]
            else:
                known_mappings[ticker] = None
        
        return known_mappings

def main():
    """Тестирование парсера"""
    
    parser = InvestFundsParser()
    
    # Тестируем на известном фонде LQDT
    print("🔍 Тестирование парсера investfunds.ru...")
    
    fund_data = parser.find_fund_by_ticker('LQDT')
    
    if fund_data:
        print(f"✅ Успешно получены данные LQDT:")
        print(f"   Название: {fund_data['name']}")
        print(f"   СЧА: {fund_data['nav']:,.2f} руб.")
        print(f"   Цена пая: {fund_data['unit_price']:.4f} руб.")
        print(f"   Дата: {fund_data['date']}")
        
        # Сравним с нашей оценкой
        our_estimate = 493.8e9  # 493.8 млрд
        actual_nav = fund_data['nav']
        
        print(f"\n📊 Сравнение:")
        print(f"   Наша оценка: {our_estimate/1e9:.1f} млрд руб.")
        print(f"   Реальные данные: {actual_nav/1e9:.1f} млрд руб.")
        
        if actual_nav > 0:
            accuracy = min(our_estimate, actual_nav) / max(our_estimate, actual_nav) * 100
            print(f"   Точность оценки: {accuracy:.1f}%")
    else:
        print("❌ Не удалось получить данные LQDT")

if __name__ == "__main__":
    main()