"""
Unit tests for Fallback Manager
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from fallback_manager import DataProviderManager
from fallback_system import DataSourceStatus, AllProvidersUnavailableError, FallbackResult


class TestDataProviderManager(unittest.TestCase):
    """Test Fallback Manager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.manager = DataProviderManager()
    
    def test_initialization(self):
        """Test manager initialization"""
        self.assertEqual(len(self.manager.providers), 3)
        
        # Check providers are sorted by priority
        priorities = [p.priority for p in self.manager.providers]
        self.assertEqual(priorities, sorted(priorities))
        
        # Check provider names
        provider_names = [p.name for p in self.manager.providers]
        self.assertIn('moex', provider_names)
        self.assertIn('yahoo_finance', provider_names)
        self.assertIn('cbr', provider_names)
    
    @patch('fallback_manager.MOEXDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.CBRDataProvider')
    def test_get_etf_data_success_from_primary(self, mock_cbr, mock_yahoo, mock_moex):
        """Test successful ETF data retrieval from primary provider (MOEX)"""
        # Setup mock MOEX provider
        mock_moex_instance = Mock()
        mock_moex_instance.name = 'moex'
        mock_moex_instance.priority = 1
        mock_moex_instance.enabled = True
        mock_moex_instance.health_check.return_value = DataSourceStatus.ACTIVE
        mock_moex_instance.get_market_data.return_value = {
            'ticker': 'SBMX',
            'current_price': 100.5,
            'currency': 'RUB'
        }
        mock_moex_instance.get_historical_data.return_value = {
            'return_annualized': 15.2,
            'volatility': 25.0
        }
        mock_moex_instance.get_trading_volume_data.return_value = {
            'avg_daily_volume': 50000
        }
        mock_moex.return_value = mock_moex_instance
        
        # Setup mock Yahoo provider (should not be called)
        mock_yahoo_instance = Mock()
        mock_yahoo_instance.name = 'yahoo_finance'
        mock_yahoo_instance.priority = 2
        mock_yahoo_instance.enabled = True
        mock_yahoo.return_value = mock_yahoo_instance
        
        # Setup mock CBR provider
        mock_cbr_instance = Mock()
        mock_cbr_instance.name = 'cbr'
        mock_cbr_instance.priority = 3
        mock_cbr_instance.enabled = True
        mock_cbr.return_value = mock_cbr_instance
        
        # Create new manager with mocked providers
        manager = DataProviderManager()
        
        result = manager.get_etf_data_with_fallback('SBMX')
        
        self.assertIsInstance(result, FallbackResult)
        self.assertEqual(result.source, 'moex')
        self.assertEqual(result.fallback_level, 0)
        self.assertIn('ticker', result.data)
        self.assertEqual(result.data['ticker'], 'SBMX')
        self.assertGreater(result.quality_score, 0)
        
        # Verify MOEX was called but Yahoo was not
        mock_moex_instance.health_check.assert_called_once()
        mock_moex_instance.get_market_data.assert_called_once_with('SBMX')
        mock_yahoo_instance.health_check.assert_not_called()
    
    @patch('fallback_manager.MOEXDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.CBRDataProvider')
    def test_get_etf_data_fallback_to_secondary(self, mock_cbr, mock_yahoo, mock_moex):
        """Test ETF data fallback to secondary provider (Yahoo Finance)"""
        # Setup mock MOEX provider (unavailable)
        mock_moex_instance = Mock()
        mock_moex_instance.name = 'moex'
        mock_moex_instance.priority = 1
        mock_moex_instance.enabled = True
        mock_moex_instance.health_check.return_value = DataSourceStatus.UNAVAILABLE
        mock_moex.return_value = mock_moex_instance
        
        # Setup mock Yahoo provider (available)
        mock_yahoo_instance = Mock()
        mock_yahoo_instance.name = 'yahoo_finance'
        mock_yahoo_instance.priority = 2
        mock_yahoo_instance.enabled = True
        mock_yahoo_instance.health_check.return_value = DataSourceStatus.ACTIVE
        mock_yahoo_instance.get_market_data.return_value = {
            'ticker': 'SBMX',
            'current_price': 101.0,
            'currency': 'RUB'
        }
        mock_yahoo_instance.get_historical_data.return_value = {
            'return_annualized': 14.8,
            'volatility': 26.0
        }
        mock_yahoo_instance.get_trading_volume_data.return_value = {
            'avg_daily_volume': 45000
        }
        mock_yahoo.return_value = mock_yahoo_instance
        
        # Setup mock CBR provider
        mock_cbr_instance = Mock()
        mock_cbr_instance.name = 'cbr'
        mock_cbr_instance.priority = 3
        mock_cbr_instance.enabled = True
        mock_cbr.return_value = mock_cbr_instance
        
        manager = DataProviderManager()
        
        result = manager.get_etf_data_with_fallback('SBMX')
        
        self.assertEqual(result.source, 'yahoo_finance')
        self.assertEqual(result.fallback_level, 1)  # Second provider
        self.assertIn('ticker', result.data)
        
        # Verify both providers were checked
        mock_moex_instance.health_check.assert_called_once()
        mock_yahoo_instance.health_check.assert_called_once()
        mock_yahoo_instance.get_market_data.assert_called_once_with('SBMX')
    
    @patch('fallback_manager.MOEXDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.CBRDataProvider')
    def test_get_etf_data_all_providers_fail(self, mock_cbr, mock_yahoo, mock_moex):
        """Test ETF data when all providers fail"""
        # Setup all providers as unavailable
        for mock_provider_class, name in [(mock_moex, 'moex'), (mock_yahoo, 'yahoo_finance'), (mock_cbr, 'cbr')]:
            mock_instance = Mock()
            mock_instance.name = name
            mock_instance.priority = 1 if name == 'moex' else (2 if name == 'yahoo_finance' else 3)
            mock_instance.enabled = True
            mock_instance.health_check.return_value = DataSourceStatus.UNAVAILABLE
            mock_provider_class.return_value = mock_instance
        
        manager = DataProviderManager()
        
        with self.assertRaises(AllProvidersUnavailableError) as context:
            manager.get_etf_data_with_fallback('SBMX')
        
        # Check that error contains information about all failed providers
        error = context.exception
        self.assertIn('moex', error.provider_errors)
        self.assertIn('yahoo_finance', error.provider_errors)
    
    @patch('fallback_manager.MOEXDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.CBRDataProvider')
    def test_get_etf_list_success(self, mock_cbr, mock_yahoo, mock_moex):
        """Test successful ETF list retrieval"""
        # Setup mock MOEX provider
        mock_moex_instance = Mock()
        mock_moex_instance.name = 'moex'
        mock_moex_instance.priority = 1
        mock_moex_instance.enabled = True
        mock_moex_instance.health_check.return_value = DataSourceStatus.ACTIVE
        mock_moex_instance.get_securities_list.return_value = [
            {'ticker': 'SBMX', 'name': 'Сбер - Мосбиржа'},
            {'ticker': 'VTBX', 'name': 'ВТБ - Мосбиржа'}
        ]
        mock_moex.return_value = mock_moex_instance
        
        # Setup other providers
        mock_yahoo.return_value = Mock(name='yahoo_finance', priority=2, enabled=True)
        mock_cbr.return_value = Mock(name='cbr', priority=3, enabled=True)
        
        manager = DataProviderManager()
        
        result = manager.get_etf_list_with_fallback()
        
        self.assertEqual(result.source, 'moex')
        self.assertIn('securities', result.data)
        self.assertEqual(len(result.data['securities']), 2)
        self.assertEqual(result.data['count'], 2)
    
    @patch('fallback_manager.CBRDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.MOEXDataProvider')
    def test_get_macro_data_success(self, mock_moex, mock_yahoo, mock_cbr):
        """Test successful macro data retrieval from CBR"""
        # Setup mock CBR provider
        mock_cbr_instance = Mock()
        mock_cbr_instance.name = 'cbr'
        mock_cbr_instance.priority = 3
        mock_cbr_instance.enabled = True
        mock_cbr_instance.health_check.return_value = DataSourceStatus.ACTIVE
        mock_cbr_instance.get_macro_indicators.return_value = {
            'currency_rates': {'USD': {'value': 75.5}},
            'key_rate': None
        }
        mock_cbr.return_value = mock_cbr_instance
        
        # Setup other providers
        mock_moex.return_value = Mock(name='moex', priority=1, enabled=True)
        mock_yahoo.return_value = Mock(name='yahoo_finance', priority=2, enabled=True)
        
        manager = DataProviderManager()
        
        result = manager.get_macro_data_with_fallback()
        
        self.assertEqual(result.source, 'cbr')
        self.assertIn('currency_rates', result.data)
        self.assertEqual(result.quality_score, 0.9)
    
    @patch('fallback_manager.CBRDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.MOEXDataProvider')
    def test_get_macro_data_cbr_unavailable(self, mock_moex, mock_yahoo, mock_cbr):
        """Test macro data when CBR is unavailable"""
        # Setup mock CBR provider as unavailable
        mock_cbr_instance = Mock()
        mock_cbr_instance.name = 'cbr'
        mock_cbr_instance.priority = 3
        mock_cbr_instance.enabled = True
        mock_cbr_instance.health_check.return_value = DataSourceStatus.UNAVAILABLE
        mock_cbr.return_value = mock_cbr_instance
        
        # Setup other providers
        mock_moex.return_value = Mock(name='moex', priority=1, enabled=True)
        mock_yahoo.return_value = Mock(name='yahoo_finance', priority=2, enabled=True)
        
        manager = DataProviderManager()
        
        result = manager.get_macro_data_with_fallback()
        
        self.assertEqual(result.source, 'fallback')
        self.assertEqual(result.fallback_level, 99)
        self.assertIn('note', result.data)
        self.assertIn('недоступны', result.data['note'])
        self.assertEqual(result.quality_score, 0.1)
    
    def test_assess_data_quality_high(self):
        """Test data quality assessment - high quality"""
        data = {
            'current_price': 100.0,
            'return_annualized': 15.0,
            'volatility': 25.0,
            'avg_daily_volume': 50000,
            'ticker': 'SBMX',
            'source': 'moex'
        }
        
        quality = self.manager._assess_data_quality(data)
        self.assertEqual(quality, 1.0)  # All key fields present
    
    def test_assess_data_quality_low(self):
        """Test data quality assessment - low quality"""
        data = {
            'ticker': 'SBMX'
            # Missing most key fields
        }
        
        quality = self.manager._assess_data_quality(data)
        self.assertLess(quality, 0.5)  # Low quality due to missing fields
    
    @patch('fallback_manager.MOEXDataProvider')
    @patch('fallback_manager.YahooFinanceProvider')
    @patch('fallback_manager.CBRDataProvider')
    def test_get_provider_status(self, mock_cbr, mock_yahoo, mock_moex):
        """Test provider status retrieval"""
        # Setup mock providers with different statuses
        mock_moex_instance = Mock()
        mock_moex_instance.name = 'moex'
        mock_moex_instance.priority = 1
        mock_moex_instance.enabled = True
        mock_moex_instance.health_check.return_value = DataSourceStatus.ACTIVE
        mock_moex_instance.metrics.response_time_ms = 150.0
        mock_moex_instance.metrics.total_requests = 100
        mock_moex_instance.metrics.error_count_1h = 2
        mock_moex.return_value = mock_moex_instance
        
        mock_yahoo_instance = Mock()
        mock_yahoo_instance.name = 'yahoo_finance'
        mock_yahoo_instance.priority = 2
        mock_yahoo_instance.enabled = True
        mock_yahoo_instance.health_check.return_value = DataSourceStatus.DEGRADED
        mock_yahoo_instance.metrics.response_time_ms = 2500.0
        mock_yahoo_instance.metrics.total_requests = 50
        mock_yahoo_instance.metrics.error_count_1h = 5
        mock_yahoo.return_value = mock_yahoo_instance
        
        mock_cbr_instance = Mock()
        mock_cbr_instance.name = 'cbr'
        mock_cbr_instance.priority = 3
        mock_cbr_instance.enabled = True
        mock_cbr_instance.health_check.return_value = DataSourceStatus.UNAVAILABLE
        mock_cbr_instance.metrics.response_time_ms = 0.0
        mock_cbr_instance.metrics.total_requests = 10
        mock_cbr_instance.metrics.error_count_1h = 10
        mock_cbr.return_value = mock_cbr_instance
        
        manager = DataProviderManager()
        
        status = manager.get_provider_status()
        
        self.assertEqual(status['total_providers'], 3)
        self.assertEqual(status['active_providers'], 2)  # ACTIVE + DEGRADED
        self.assertIn('providers', status)
        self.assertEqual(len(status['providers']), 3)
        
        # Check individual provider status
        moex_status = next(p for p in status['providers'] if p['name'] == 'moex')
        self.assertEqual(moex_status['status'], 'active')
        self.assertEqual(moex_status['priority'], 1)
        
        yahoo_status = next(p for p in status['providers'] if p['name'] == 'yahoo_finance')
        self.assertEqual(yahoo_status['status'], 'degraded')
        
        cbr_status = next(p for p in status['providers'] if p['name'] == 'cbr')
        self.assertEqual(cbr_status['status'], 'unavailable')


if __name__ == '__main__':
    unittest.main()