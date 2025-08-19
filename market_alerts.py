"""
Система алертов для мониторинга российского рынка БПИФ
Отслеживает новые фонды и крупные движения капитала
"""

import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketAlerts:
    """Система алертов для мониторинга рынка БПИФ"""
    
    def __init__(self, data_dir: str = "./alerts_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Файлы для хранения истории
        self.funds_history_file = self.data_dir / "funds_history.json"
        self.nav_history_file = self.data_dir / "nav_history.json"
        self.alerts_file = self.data_dir / "active_alerts.json"
        
        # Пороги для алертов
        self.CAPITAL_FLOW_THRESHOLD = 0.05  # 5% от СЧА
        
        # Инициализация файлов
        self._init_storage_files()
    
    def _init_storage_files(self):
        """Инициализирует файлы для хранения данных"""
        for file_path in [self.funds_history_file, self.nav_history_file, self.alerts_file]:
            if not file_path.exists():
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump({}, f, ensure_ascii=False)
    
    def check_new_funds(self, current_funds: pd.DataFrame) -> List[Dict]:
        """
        Проверяет появление новых фондов на ММВБ
        
        Args:
            current_funds: DataFrame с текущими фондами
            
        Returns:
            List[Dict]: Список алертов о новых фондах
        """
        alerts = []
        current_time = datetime.now().isoformat()
        
        try:
            # Загружаем историю фондов
            with open(self.funds_history_file, 'r', encoding='utf-8') as f:
                funds_history = json.load(f)
            
            # Получаем список тикеров
            current_tickers = set(current_funds['ticker'].tolist())
            
            # Проверяем есть ли история
            last_check_date = funds_history.get('last_update', '')
            previous_tickers = set(funds_history.get('tickers', []))
            
            # Находим новые фонды
            new_tickers = current_tickers - previous_tickers
            
            if new_tickers and last_check_date:
                for ticker in new_tickers:
                    fund_info = current_funds[current_funds['ticker'] == ticker].iloc[0]
                    
                    alert = {
                        'type': 'NEW_FUND',
                        'timestamp': current_time,
                        'ticker': ticker,
                        'name': fund_info.get('name', 'Неизвестно'),
                        'category': fund_info.get('category', 'Неизвестно'),
                        'management_company': fund_info.get('management_company', 'Неизвестно'),
                        'inception_date': fund_info.get('inception_date', 'Неизвестно'),
                        'message': f"🆕 Новый фонд {ticker} появился на ММВБ!",
                        'priority': 'HIGH'
                    }
                    alerts.append(alert)
                    logger.info(f"Обнаружен новый фонд: {ticker}")
            
            # Обновляем историю
            funds_history.update({
                'last_update': current_time,
                'tickers': list(current_tickers),
                'total_funds': len(current_tickers)
            })
            
            with open(self.funds_history_file, 'w', encoding='utf-8') as f:
                json.dump(funds_history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Ошибка проверки новых фондов: {e}")
        
        return alerts
    
    def check_capital_flows(self, current_funds: pd.DataFrame) -> List[Dict]:
        """
        Проверяет крупные изменения СЧА фондов (>5%)
        
        Args:
            current_funds: DataFrame с текущими данными фондов
            
        Returns:
            List[Dict]: Список алертов о движениях капитала
        """
        alerts = []
        current_time = datetime.now().isoformat()
        
        try:
            # Загружаем историю СЧА
            with open(self.nav_history_file, 'r', encoding='utf-8') as f:
                nav_history = json.load(f)
            
            # Получаем данные по СЧА с investfunds.ru
            from investfunds_parser import InvestFundsParser
            parser = InvestFundsParser()
            
            for _, fund in current_funds.iterrows():
                ticker = fund['ticker']
                
                # Получаем текущее СЧА
                real_data = parser.find_fund_by_ticker(ticker)
                if not real_data or not real_data.get('nav'):
                    continue
                
                current_nav = real_data['nav']
                fund_name = fund.get('name', ticker)
                
                # Проверяем есть ли история для этого фонда
                if ticker in nav_history:
                    previous_nav = nav_history[ticker].get('last_nav')
                    last_update = nav_history[ticker].get('last_update')
                    
                    if previous_nav and previous_nav > 0:
                        # Рассчитываем изменение в процентах
                        change_percent = (current_nav - previous_nav) / previous_nav
                        
                        # Проверяем превышение порога
                        if abs(change_percent) >= self.CAPITAL_FLOW_THRESHOLD:
                            flow_type = "ПРИТОК" if change_percent > 0 else "ОТТОК"
                            change_billions = (current_nav - previous_nav) / 1_000_000_000
                            
                            alert = {
                                'type': 'CAPITAL_FLOW',
                                'timestamp': current_time,
                                'ticker': ticker,
                                'name': fund_name,
                                'flow_type': flow_type,
                                'change_percent': round(change_percent * 100, 2),
                                'change_amount_billions': round(change_billions, 2),
                                'previous_nav_billions': round(previous_nav / 1_000_000_000, 2),
                                'current_nav_billions': round(current_nav / 1_000_000_000, 2),
                                'last_update': last_update,
                                'message': f"💰 {flow_type} капитала в {ticker}: {abs(change_percent)*100:.1f}% ({abs(change_billions):.1f} млрд ₽)",
                                'priority': 'HIGH' if abs(change_percent) >= 0.10 else 'MEDIUM'
                            }
                            alerts.append(alert)
                            logger.info(f"Крупное движение капитала: {ticker} {change_percent*100:.1f}%")
                
                # Обновляем историю для фонда
                nav_history[ticker] = {
                    'last_nav': current_nav,
                    'last_update': current_time,
                    'fund_name': fund_name
                }
            
            # Сохраняем обновленную историю
            with open(self.nav_history_file, 'w', encoding='utf-8') as f:
                json.dump(nav_history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Ошибка проверки движений капитала: {e}")
        
        return alerts
    
    def get_market_anomalies(self, current_funds: pd.DataFrame) -> List[Dict]:
        """
        Выявляет рыночные аномалии и необычные события
        
        Args:
            current_funds: DataFrame с текущими данными
            
        Returns:
            List[Dict]: Список алертов об аномалиях
        """
        alerts = []
        current_time = datetime.now().isoformat()
        
        try:
            # 1. Аномально высокие объемы торгов
            median_volume = current_funds['avg_daily_volume'].median()
            high_volume_threshold = median_volume * 5  # В 5 раз выше медианы
            
            high_volume_funds = current_funds[
                current_funds['avg_daily_volume'] > high_volume_threshold
            ]
            
            for _, fund in high_volume_funds.iterrows():
                volume_ratio = fund['avg_daily_volume'] / median_volume
                alert = {
                    'type': 'VOLUME_ANOMALY',
                    'timestamp': current_time,
                    'ticker': fund['ticker'],
                    'name': fund.get('name', 'Неизвестно'),
                    'volume_ratio': round(volume_ratio, 1),
                    'daily_volume': int(fund['avg_daily_volume']),
                    'message': f"📈 Аномально высокий объем торгов {fund['ticker']}: в {volume_ratio:.1f} раз выше медианы",
                    'priority': 'MEDIUM'
                }
                alerts.append(alert)
            
            # 2. Экстремальная волатильность
            volatility_threshold = 50  # Более 50%
            high_volatility_funds = current_funds[
                current_funds['volatility'] > volatility_threshold
            ]
            
            for _, fund in high_volatility_funds.iterrows():
                alert = {
                    'type': 'VOLATILITY_ANOMALY',
                    'timestamp': current_time,
                    'ticker': fund['ticker'],
                    'name': fund.get('name', 'Неизвестно'),
                    'volatility': round(fund['volatility'], 1),
                    'message': f"⚠️ Экстремальная волатильность {fund['ticker']}: {fund['volatility']:.1f}%",
                    'priority': 'MEDIUM'
                }
                alerts.append(alert)
            
            # 3. Экстремальная доходность (очень высокая или очень низкая)
            extreme_returns = current_funds[
                (current_funds['annual_return'] > 100) | 
                (current_funds['annual_return'] < -50)
            ]
            
            for _, fund in extreme_returns.iterrows():
                return_type = "ВЫСОКАЯ" if fund['annual_return'] > 0 else "НИЗКАЯ"
                alert = {
                    'type': 'RETURN_ANOMALY',
                    'timestamp': current_time,
                    'ticker': fund['ticker'],
                    'name': fund.get('name', 'Неизвестно'),
                    'annual_return': round(fund['annual_return'], 1),
                    'return_type': return_type,
                    'message': f"🎯 Экстремальная доходность {fund['ticker']}: {fund['annual_return']:.1f}%",
                    'priority': 'LOW'
                }
                alerts.append(alert)
                
        except Exception as e:
            logger.error(f"Ошибка выявления аномалий: {e}")
        
        return alerts
    
    def save_alerts(self, alerts: List[Dict]):
        """Сохраняет алерты в файл"""
        try:
            with open(self.alerts_file, 'r', encoding='utf-8') as f:
                existing_alerts = json.load(f)
            
            # Добавляем новые алерты
            for alert in alerts:
                alert_id = f"{alert['type']}_{alert['ticker']}_{alert['timestamp']}"
                existing_alerts[alert_id] = alert
            
            # Очищаем старые алерты (старше 7 дней)
            cutoff_date = datetime.now() - timedelta(days=7)
            filtered_alerts = {
                k: v for k, v in existing_alerts.items()
                if datetime.fromisoformat(v['timestamp']) > cutoff_date
            }
            
            with open(self.alerts_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_alerts, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Ошибка сохранения алертов: {e}")
    
    def get_active_alerts(self, hours: int = 24) -> List[Dict]:
        """
        Получает активные алерты за последние N часов
        
        Args:
            hours: Количество часов для фильтрации
            
        Returns:
            List[Dict]: Список активных алертов
        """
        try:
            with open(self.alerts_file, 'r', encoding='utf-8') as f:
                all_alerts = json.load(f)
            
            # Фильтруем по времени
            cutoff_time = datetime.now() - timedelta(hours=hours)
            active_alerts = [
                alert for alert in all_alerts.values()
                if datetime.fromisoformat(alert['timestamp']) > cutoff_time
            ]
            
            # Сортируем по времени (новые сначала)
            active_alerts.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return active_alerts
            
        except Exception as e:
            logger.error(f"Ошибка получения алертов: {e}")
            return []
    
    def run_full_scan(self, current_funds: pd.DataFrame) -> Dict:
        """
        Запускает полное сканирование рынка и генерирует все типы алертов
        
        Args:
            current_funds: DataFrame с текущими данными фондов
            
        Returns:
            Dict: Сводка результатов сканирования
        """
        logger.info("🔍 Запуск полного сканирования рынка БПИФ...")
        
        all_alerts = []
        
        # 1. Проверка новых фондов
        new_fund_alerts = self.check_new_funds(current_funds)
        all_alerts.extend(new_fund_alerts)
        
        # 2. Проверка движений капитала
        capital_flow_alerts = self.check_capital_flows(current_funds)
        all_alerts.extend(capital_flow_alerts)
        
        # 3. Проверка аномалий
        anomaly_alerts = self.get_market_anomalies(current_funds)
        all_alerts.extend(anomaly_alerts)
        
        # Сохраняем все алерты
        if all_alerts:
            self.save_alerts(all_alerts)
        
        # Формируем сводку
        summary = {
            'scan_timestamp': datetime.now().isoformat(),
            'total_funds_scanned': len(current_funds),
            'total_alerts': len(all_alerts),
            'new_funds': len(new_fund_alerts),
            'capital_flows': len(capital_flow_alerts),
            'anomalies': len(anomaly_alerts),
            'alerts': all_alerts
        }
        
        logger.info(f"✅ Сканирование завершено: {len(all_alerts)} алертов")
        
        return summary

if __name__ == "__main__":
    # Тестирование системы алертов
    print("🚨 Тест системы алертов...")
    
    # Загружаем текущие данные
    import pandas as pd
    from pathlib import Path
    
    data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
    if data_files:
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        current_funds = pd.read_csv(latest_data)
        
        # Инициализируем систему алертов
        alerts_system = MarketAlerts()
        
        # Запускаем сканирование
        results = alerts_system.run_full_scan(current_funds)
        
        print(f"📊 Результаты сканирования:")
        print(f"   Фондов проверено: {results['total_funds_scanned']}")
        print(f"   Алертов создано: {results['total_alerts']}")
        print(f"   Новые фонды: {results['new_funds']}")
        print(f"   Движения капитала: {results['capital_flows']}")
        print(f"   Аномалии: {results['anomalies']}")
        
    else:
        print("❌ Не найдены файлы с данными ETF")