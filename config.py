"""
Конфигурация для системы анализа ETF
"""
import os

class Config:
    def __init__(self):
        self.base_path = os.getcwd()
        self.moex = {
            'base_url': 'https://iss.moex.com/iss',
            'timeout': 30,
            'retry_attempts': 3
        }
        self.investfunds = {
            'base_url': 'https://investfunds.ru',
            'timeout': 30,
            'cache_duration': 3600  # 1 час
        }
        self.data = {
            'update_frequency': 'daily',
            'cache_dir': 'cache',
            'historical_days': 365
        }

# Глобальный объект конфигурации
config = Config()

# Известные ETF тикеры (основные российские БПИФ)
KNOWN_ETFS = [
    'LQDT', 'AKMB', 'VTBX', 'SBGB', 'TRUR', 'DIVD', 'RUSE',
    'RUSB', 'SBCB', 'VTBB', 'SBGB', 'RGBI', 'RUCB', 'RUCB',
    'RUSD', 'RUSE', 'RUSM', 'RUSS', 'RUST', 'RUSP', 'CHMF',
    'AKBC', 'AKFB', 'AKGD', 'AKHT', 'AKIE', 'AKME', 'AKMM',
    'AKMP', 'AKPP', 'AKQU', 'AKRU', 'AKTR', 'AKUP', 'AKUS',
    'RUSI', 'RUSS', 'VTBU', 'VTBE', 'AKGP', 'AKAI', 'SCFT'
]

# Настройки логирования
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'filename': 'etf_analysis.log'
}

# Обертка для совместимости с простым дашбордом
class MOEXProvider:
    """Простая обертка для совместимости с dashboard"""
    
    def __init__(self):
        try:
            from moex_provider import MOEXDataProvider
            self.provider = MOEXDataProvider()
        except:
            self.provider = None
    
    def get_all_etfs(self):
        """Возвращает список ETF в формате для дашборда"""
        if not self.provider:
            return []
        
        try:
            # Возвращаем базовый список известных БПИФ
            etfs = []
            for ticker in KNOWN_ETFS:
                etfs.append({
                    'ticker': ticker,
                    'name': f'БПИФ {ticker}',
                    'current_price': 100.0,
                    'volume': 1000000
                })
            return etfs
        except Exception as e:
            print(f"Ошибка получения ETF: {e}")
            return []