"""
Сбор реальных данных о российских БПИФах с доступных источников
"""

import requests
import pandas as pd
import json
from datetime import datetime
import time

class RealETFDataCollector:
    """
    Класс для сбора актуальных данных о российских БПИФах
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.collected_data = []
        
    def collect_moex_etf_data(self):
        """
        Сбор данных о ETF с Московской биржи через API
        """
        print("Попытка получения данных с MOEX API...")
        
        try:
            # URL для получения списка ETF на MOEX
            url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQTF/securities.json"
            
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if 'securities' in data and 'data' in data['securities']:
                    securities_data = data['securities']['data']
                    columns = data['securities']['columns']
                    
                    etf_list = []
                    for row in securities_data:
                        etf_info = dict(zip(columns, row))
                        if etf_info.get('SECTYPE') == 'ETF':
                            etf_list.append({
                                'ticker': etf_info.get('SECID'),
                                'name': etf_info.get('SHORTNAME'),
                                'full_name': etf_info.get('SECNAME'),
                                'lot_size': etf_info.get('LOTSIZE'),
                                'currency': 'RUB',
                                'source': 'MOEX'
                            })
                    
                    print(f"Найдено {len(etf_list)} ETF на MOEX")
                    return etf_list
                    
            return []
            
        except Exception as e:
            print(f"Ошибка при получении данных MOEX: {e}")
            return []
    
    def get_etf_market_data(self, ticker):
        """
        Получение рыночных данных по тикеру
        """
        try:
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'securities' in data and 'data' in data['securities']:
                    sec_data = data['securities']['data'][0] if data['securities']['data'] else []
                    columns = data['securities']['columns']
                    
                    if sec_data:
                        market_info = dict(zip(columns, sec_data))
                        return {
                            'last_price': market_info.get('PREVPRICE'),
                            'market_cap': market_info.get('ISSUESIZE'),
                            'lot_size': market_info.get('LOTSIZE')
                        }
            
            return {}
            
        except Exception as e:
            print(f"Ошибка получения данных для {ticker}: {e}")
            return {}
    
    def collect_known_russian_etfs(self):
        """
        Сбор данных об известных российских ETF
        """
        print("Сбор данных об известных российских ETF...")
        
        # Известные российские ETF тикеры
        known_etfs = [
            'SBMX', 'VTBX', 'TMOS', 'TECH', 'TGLD', 'TIPO', 'DIVD',
            'FXRU', 'FXUS', 'FXCN', 'FXDE', 'FXGD', 'FXIT', 'FXWO',
            'RUSB', 'RUSD', 'RUSE', 'RUSG', 'SBGB', 'SBCB', 'SBRF'
        ]
        
        etf_data = []
        
        for ticker in known_etfs:
            print(f"Получение данных для {ticker}...")
            
            market_data = self.get_etf_market_data(ticker)
            
            etf_info = {
                'ticker': ticker,
                'last_price': market_data.get('last_price'),
                'market_cap_shares': market_data.get('market_cap'),
                'lot_size': market_data.get('lot_size'),
                'data_source': 'MOEX_API',
                'collection_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            etf_data.append(etf_info)
            time.sleep(0.5)  # Пауза между запросами
            
        return etf_data
    
    def get_management_companies_info(self):
        """
        Получение информации об управляющих компаниях
        """
        print("Сбор информации об управляющих компаниях...")
        
        # Известные российские УК и их фонды
        uk_info = {
            'Сбер Управление Активами': {
                'etfs': ['SBMX', 'SBGB', 'SBCB', 'SBRF'],
                'website': 'sber-am.ru',
                'description': 'Управляющая компания Сбербанка'
            },
            'ВТБ Капитал Управление Активами': {
                'etfs': ['VTBX'],
                'website': 'vtbcapital-am.ru', 
                'description': 'Управляющая компания ВТБ'
            },
            'Тинькофф Капитал': {
                'etfs': ['TECH', 'TGLD', 'TIPO'],
                'website': 'tinkoff.ru',
                'description': 'Управляющая компания Тинькофф'
            },
            'FinEx': {
                'etfs': ['FXRU', 'FXUS', 'FXCN', 'FXDE', 'FXGD', 'FXIT', 'FXWO'],
                'website': 'finex-etf.ru',
                'description': 'Специализированная ETF компания'
            },
            'УК Альфа-Капитал': {
                'etfs': ['DIVD'],
                'website': 'alfacapital.ru',
                'description': 'Управляющая компания Альфа-Банка'
            }
        }
        
        return uk_info
    
    def create_comprehensive_dataset(self):
        """
        Создание комплексного датасета на основе реальных данных
        """
        print("Создание комплексного датасета...")
        
        # Получаем данные с MOEX
        moex_etfs = self.collect_moex_etf_data()
        
        # Получаем данные по известным ETF
        known_etfs = self.collect_known_russian_etfs()
        
        # Информация об УК
        uk_info = self.get_management_companies_info()
        
        # Объединяем данные
        comprehensive_data = []
        
        for etf in known_etfs:
            ticker = etf['ticker']
            
            # Определяем управляющую компанию
            management_company = 'Неизвестно'
            for uk, info in uk_info.items():
                if ticker in info['etfs']:
                    management_company = uk
                    break
            
            # Категоризация по тикеру (упрощенная)
            category = self.categorize_etf(ticker)
            
            comprehensive_data.append({
                'ticker': ticker,
                'management_company': management_company,
                'category': category,
                'last_price': etf.get('last_price'),
                'market_cap_shares': etf.get('market_cap_shares'),
                'lot_size': etf.get('lot_size'),
                'data_date': etf.get('collection_date'),
                'data_source': 'Real MOEX API'
            })
        
        return pd.DataFrame(comprehensive_data)
    
    def categorize_etf(self, ticker):
        """
        Категоризация ETF по тикеру
        """
        if ticker in ['SBMX', 'VTBX', 'TMOS', 'FXRU']:
            return 'Российские акции'
        elif ticker in ['TECH', 'FXIT']:
            return 'Технологические акции'
        elif ticker in ['TGLD', 'FXGD']:
            return 'Драгоценные металлы'
        elif ticker in ['FXUS', 'FXWO']:
            return 'Зарубежные акции'
        elif ticker in ['SBGB', 'RUSB']:
            return 'Государственные облигации'
        elif ticker in ['SBCB', 'RUSD']:
            return 'Корпоративные облигации'
        elif ticker in ['DIVD']:
            return 'Дивидендные акции'
        else:
            return 'Прочие'
    
    def save_real_data(self, filename='real_russian_etf_data.csv'):
        """
        Сохранение реальных данных
        """
        df = self.create_comprehensive_dataset()
        
        filepath = f'/Users/tumowuh/Desktop/market analysis/{filename}'
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        print(f"Реальные данные сохранены в: {filepath}")
        print(f"Количество ETF: {len(df)}")
        print("\nПредварительный просмотр данных:")
        print(df.head())
        
        return df

def main():
    """
    Основная функция для сбора данных
    """
    print("=== СБОР РЕАЛЬНЫХ ДАННЫХ О РОССИЙСКИХ БПИФ ===\n")
    
    collector = RealETFDataCollector()
    
    try:
        # Сбор и сохранение данных
        real_data = collector.save_real_data()
        
        print("\n=== СТАТИСТИКА ПО СОБРАННЫМ ДАННЫМ ===")
        print(f"Всего ETF собрано: {len(real_data)}")
        
        # Группировка по УК
        uk_stats = real_data['management_company'].value_counts()
        print("\nРаспределение по управляющим компаниям:")
        print(uk_stats)
        
        # Группировка по категориям
        category_stats = real_data['category'].value_counts()
        print("\nРаспределение по категориям:")
        print(category_stats)
        
        # Информация о ценах
        price_info = real_data['last_price'].describe()
        print("\nСтатистика по ценам:")
        print(price_info)
        
    except Exception as e:
        print(f"Ошибка при сборе данных: {e}")
        print("Проверьте подключение к интернету и доступность API MOEX")

if __name__ == "__main__":
    main()