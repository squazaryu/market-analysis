"""
Unit tests for MOEX Data Provider
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
from datetime import datetime
import pandas as pd

from moex_provider import MOEXDataProvider
from fallback_system import DataSourceStatus


class TestMOEXDataProvider(unittest.TestCase):
    """Test MOEX Data Provider functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.provider = MOEXDataProvider()
    
    def test_initialization(self):
        """Test MOEX provider initialization"""
        self.assertEqual(self.provider.name, "moex")
        self.assertEqual(self.provider.priority, 1)
        self.assertTrue(self.provider.enabled)
        self.assertIn('base_url', self.provider.config)
        self.assertEqual(self.provider.config['base_url'], 'https://iss.moex.com/iss')
    
    def test_custom_config_initialization(self):
        """Test MOEX provider with custom config"""
        custom_config = {
            'base_url': 'https://custom.moex.com/iss',
            'timeout': 20,
            'retry_attempts': 5
        }
        
        provider = MOEXDataProvider(custom_config)
        self.assertEqual(provider.config, custom_config)
        self.assertEqual(provider.config['timeout'], 20)
    
    @patch('moex_provider.requests.Session.get')
    def test_health_check_active(self, mock_get):
        """Test health check returns ACTIVE for fast response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 0.5]):  # 500ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.ACTIVE)
        mock_get.assert_called_once()
    
    @patch('moex_provider.requests.Session.get')
    def test_health_check_degraded(self, mock_get):
        """Test health check returns DEGRADED for slow response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 2.0]):  # 2000ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.DEGRADED)
    
    @patch('moex_provider.requests.Session.get')
    def test_health_check_unavailable_http_error(self, mock_get):
        """Test health check returns UNAVAILABLE for HTTP error"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.UNAVAILABLE)
    
    @patch('moex_provider.requests.Session.get')
    def test_health_check_unavailable_exception(self, mock_get):
        """Test health check returns UNAVAILABLE for connection error"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.UNAVAILABLE)
    
    def test_health_check_caching(self):
        """Test health check result is cached"""
        with patch.object(self.provider, '_make_request_with_retry') as mock_request:
            with patch('moex_provider.requests.Session.get') as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_get.return_value = mock_response
                
                # First call
                status1 = self.provider.health_check()
                # Second call should use cache
                status2 = self.provider.health_check()
                
                self.assertEqual(status1, status2)
                # Should only make one HTTP request due to caching
                self.assertEqual(mock_get.call_count, 1)
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_securities_list_success(self, mock_request):
        """Test successful securities list retrieval"""
        mock_data = {
            'securities': {
                'columns': ['SECID', 'SHORTNAME', 'SECNAME', 'LOTSIZE', 'SECTYPE', 'FACEUNIT'],
                'data': [
                    ['SBER', 'Сбербанк', 'Сбербанк России ПАО ао', 10, 'common_share', 'RUB'],
                    ['SBMX', 'СберМосБ', 'Сбер - Мосбиржа ETF', 1, 'ETF', 'RUB'],
                    ['VTBX', 'ВТБМосБ', 'ВТБ - Мосбиржа ETF', 1, 'ETF', 'RUB']
                ]
            }
        }
        mock_request.return_value = mock_data
        
        securities = self.provider.get_securities_list()
        
        # Should only return ETFs
        self.assertEqual(len(securities), 2)
        self.assertEqual(securities[0]['ticker'], 'SBMX')
        self.assertEqual(securities[1]['ticker'], 'VTBX')
        
        # Check data structure
        for security in securities:
            self.assertIn('ticker', security)
            self.assertIn('name', security)
            self.assertIn('source', security)
            self.assertEqual(security['source'], 'MOEX')
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_securities_list_empty_response(self, mock_request):
        """Test securities list with empty response"""
        mock_request.return_value = None
        
        securities = self.provider.get_securities_list()
        self.assertEqual(securities, [])
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_securities_list_malformed_data(self, mock_request):
        """Test securities list with malformed data"""
        mock_request.return_value = {'invalid': 'data'}
        
        securities = self.provider.get_securities_list()
        self.assertEqual(securities, [])
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_market_data_success(self, mock_request):
        """Test successful market data retrieval"""
        mock_data = {
            'securities': {
                'columns': ['SECID', 'PREVPRICE', 'ISSUESIZE', 'LOTSIZE', 'FACEUNIT', 'TRADINGSTATUS'],
                'data': [['SBMX', 100.5, 1000000, 1, 'RUB', 'T']]
            },
            'marketdata': {
                'columns': ['SECID', 'LAST', 'BID', 'OFFER', 'VOLTODAY', 'VALTODAY', 'CHANGE', 'LASTTOPREVPRICE'],
                'data': [['SBMX', 101.0, 100.8, 101.2, 50000, 5050000, 0.5, 0.5]]
            }
        }
        mock_request.return_value = mock_data
        
        market_data = self.provider.get_market_data('SBMX')
        
        self.assertEqual(market_data['ticker'], 'SBMX')
        self.assertEqual(market_data['last_price'], 100.5)
        self.assertEqual(market_data['current_price'], 101.0)
        self.assertEqual(market_data['source'], 'MOEX')
        self.assertIn('timestamp', market_data)
        self.assertIn('data_quality', market_data)
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_market_data_empty_response(self, mock_request):
        """Test market data with empty response"""
        mock_request.return_value = None
        
        market_data = self.provider.get_market_data('SBMX')
        self.assertEqual(market_data, {})
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_historical_data_success(self, mock_request):
        """Test successful historical data retrieval"""
        # Mock candles data
        mock_data = {
            'candles': {
                'columns': ['begin', 'open', 'close', 'high', 'low', 'value', 'volume'],
                'data': [
                    ['2023-01-01T00:00:00', 100, 105, 106, 99, 1000000, 10000],
                    ['2023-01-02T00:00:00', 105, 110, 112, 104, 1100000, 11000],
                    ['2023-01-03T00:00:00', 110, 108, 115, 107, 1200000, 12000]
                ]
            }
        }
        mock_request.return_value = mock_data
        
        historical_data = self.provider.get_historical_data('SBMX', days=30)
        
        self.assertEqual(historical_data['ticker'], 'SBMX')
        self.assertIn('return_period', historical_data)
        self.assertIn('volatility', historical_data)
        self.assertIn('max_drawdown', historical_data)
        self.assertEqual(historical_data['data_points'], 3)
        self.assertEqual(historical_data['source'], 'MOEX')
    
    @patch.object(MOEXDataProvider, '_make_request_with_retry')
    def test_get_trading_volume_data_success(self, mock_request):
        """Test successful trading volume data retrieval"""
        mock_data = {
            'candles': {
                'columns': ['begin', 'open', 'close', 'high', 'low', 'value', 'volume'],
                'data': [
                    ['2023-01-01T00:00:00', 100, 105, 106, 99, 1000000, 10000],
                    ['2023-01-02T00:00:00', 105, 110, 112, 104, 1100000, 11000],
                    ['2023-01-03T00:00:00', 110, 108, 115, 107, 0, 0]  # No trading day
                ]
            }
        }
        mock_request.return_value = mock_data
        
        volume_data = self.provider.get_trading_volume_data('SBMX')
        
        self.assertEqual(volume_data['ticker'], 'SBMX')
        self.assertIn('avg_daily_volume', volume_data)
        self.assertIn('avg_daily_value_rub', volume_data)
        self.assertEqual(volume_data['trading_days'], 2)  # Only 2 days with volume > 0
        self.assertEqual(volume_data['total_days'], 3)
        self.assertAlmostEqual(volume_data['liquidity_score'], 2/3, places=2)
    
    def test_get_source_info(self):
        """Test source info retrieval"""
        info = self.provider.get_source_info()
        
        self.assertEqual(info['name'], 'moex')
        self.assertEqual(info['display_name'], 'Московская биржа')
        self.assertEqual(info['type'], 'official_api')
        self.assertEqual(info['data_quality'], 'high')
        self.assertTrue(info['real_time'])
        self.assertIn('base_url', info)
        self.assertIn('status', info)
    
    @patch('moex_provider.requests.Session.get')
    def test_make_request_with_retry_success(self, mock_get):
        """Test successful request with retry mechanism"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"test": "data"}'
        mock_response.json.return_value = {"test": "data"}
        mock_get.return_value = mock_response
        
        result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertEqual(result, {"test": "data"})
        mock_get.assert_called_once()
    
    @patch('moex_provider.requests.Session.get')
    def test_make_request_with_retry_timeout_then_success(self, mock_get):
        """Test request retry after timeout"""
        # First call times out, second succeeds
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"test": "data"}'
        mock_response.json.return_value = {"test": "data"}
        
        mock_get.side_effect = [
            requests.exceptions.Timeout("Timeout"),
            mock_response
        ]
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertEqual(result, {"test": "data"})
        self.assertEqual(mock_get.call_count, 2)
    
    @patch('moex_provider.requests.Session.get')
    def test_make_request_with_retry_all_attempts_fail(self, mock_get):
        """Test request failure after all retry attempts"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)  # Default retry_attempts
    
    @patch('moex_provider.requests.Session.get')
    def test_make_request_with_retry_http_error_4xx(self, mock_get):
        """Test request with 4xx HTTP error (no retry)"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 1)  # No retry for 4xx errors
    
    @patch('moex_provider.requests.Session.get')
    def test_make_request_with_retry_http_error_5xx(self, mock_get):
        """Test request with 5xx HTTP error (with retry)"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)  # Retry for 5xx errors
    
    def test_assess_market_data_quality_high(self):
        """Test market data quality assessment - high quality"""
        data = {
            'last_price': 100.0,
            'volume': 50000,
            'bid': 99.8,
            'ask': 100.2,
            'trading_status': 'T',
            'timestamp': datetime.now().isoformat()
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 1.0)  # All checks pass
    
    def test_assess_market_data_quality_low(self):
        """Test market data quality assessment - low quality"""
        data = {
            'ticker': 'TEST'
            # Missing most required fields
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 0.0)  # No checks pass
    
    def test_assess_market_data_quality_medium(self):
        """Test market data quality assessment - medium quality"""
        data = {
            'last_price': 100.0,
            'volume': 50000,
            'timestamp': datetime.now().isoformat()
            # Missing bid/ask and trading_status
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 0.6)  # 3 out of 5 checks pass


if __name__ == '__main__':
    unittest.main()