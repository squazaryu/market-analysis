"""
Unit tests for Central Bank of Russia Data Provider
"""

import unittest
from unittest.mock import Mock, patch
import requests
from datetime import datetime, timedelta

from cbr_provider import CBRDataProvider
from fallback_system import DataSourceStatus


class TestCBRDataProvider(unittest.TestCase):
    """Test CBR Data Provider functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.provider = CBRDataProvider()
    
    def test_initialization(self):
        """Test CBR provider initialization"""
        self.assertEqual(self.provider.name, "cbr")
        self.assertEqual(self.provider.priority, 3)
        self.assertTrue(self.provider.enabled)
        self.assertIn('base_url', self.provider.config)
        self.assertEqual(self.provider.config['base_url'], 'https://www.cbr-xml-daily.ru/api')
        self.assertIn('USD', self.provider.supported_currencies)
        self.assertIn('EUR', self.provider.supported_currencies)
    
    def test_custom_config_initialization(self):
        """Test CBR provider with custom config"""
        custom_config = {
            'base_url': 'https://custom.cbr.ru/api',
            'timeout': 20,
            'data_types': ['currency_only']
        }
        
        provider = CBRDataProvider(custom_config)
        self.assertEqual(provider.config, custom_config)
        self.assertEqual(provider.config['timeout'], 20)
    
    @patch('cbr_provider.requests.Session.get')
    def test_health_check_active(self, mock_get):
        """Test health check returns ACTIVE for fast response with valid data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'Valute': {
                'USD': {'Name': 'Доллар США', 'Value': 75.5}
            }
        }
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 1.0]):  # 1000ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.ACTIVE)
    
    @patch('cbr_provider.requests.Session.get')
    def test_health_check_degraded_slow_response(self, mock_get):
        """Test health check returns DEGRADED for slow response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'Valute': {
                'USD': {'Name': 'Доллар США', 'Value': 75.5}
            }
        }
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 3.0]):  # 3000ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.DEGRADED)
    
    @patch('cbr_provider.requests.Session.get')
    def test_health_check_degraded_invalid_data(self, mock_get):
        """Test health check returns DEGRADED for invalid data structure"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'invalid': 'structure'}
        mock_get.return_value = mock_response
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.DEGRADED)
    
    @patch('cbr_provider.requests.Session.get')
    def test_health_check_unavailable(self, mock_get):
        """Test health check returns UNAVAILABLE for connection error"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.UNAVAILABLE)
    
    def test_health_check_caching(self):
        """Test health check result is cached"""
        with patch('cbr_provider.requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'Valute': {
                    'USD': {'Name': 'Доллар США', 'Value': 75.5}
                }
            }
            mock_get.return_value = mock_response
            
            # First call
            status1 = self.provider.health_check()
            # Second call should use cache
            status2 = self.provider.health_check()
            
            self.assertEqual(status1, status2)
            # Should only make one HTTP request due to caching
            self.assertEqual(mock_get.call_count, 1)
    
    def test_get_securities_list_empty(self):
        """Test securities list returns empty (not supported by CBR)"""
        securities = self.provider.get_securities_list()
        self.assertEqual(securities, [])
    
    def test_get_market_data_empty(self):
        """Test market data returns empty (not supported by CBR)"""
        market_data = self.provider.get_market_data('SBER')
        self.assertEqual(market_data, {})
    
    def test_get_historical_data_empty(self):
        """Test historical data returns empty (not supported by CBR)"""
        historical_data = self.provider.get_historical_data('SBER')
        self.assertEqual(historical_data, {})
    
    def test_get_trading_volume_data_empty(self):
        """Test trading volume data returns empty (not supported by CBR)"""
        volume_data = self.provider.get_trading_volume_data('SBER')
        self.assertEqual(volume_data, {})
    
    @patch.object(CBRDataProvider, '_make_request_with_retry')
    def test_get_currency_rates_current(self, mock_request):
        """Test successful current currency rates retrieval"""
        mock_data = {
            'Date': '2024-01-15T11:30:00+03:00',
            'Timestamp': '2024-01-15T11:30:00+03:00',
            'Valute': {
                'USD': {
                    'Name': 'Доллар США',
                    'Nominal': 1,
                    'Value': 75.5,
                    'Previous': 75.0
                },
                'EUR': {
                    'Name': 'Евро',
                    'Nominal': 1,
                    'Value': 85.2,
                    'Previous': 84.8
                }
            }
        }
        mock_request.return_value = mock_data
        
        rates = self.provider.get_currency_rates()
        
        self.assertEqual(rates['source'], 'CBR')
        self.assertIn('rates', rates)
        self.assertIn('USD', rates['rates'])
        self.assertIn('EUR', rates['rates'])
        
        usd_rate = rates['rates']['USD']
        self.assertEqual(usd_rate['value'], 75.5)
        self.assertEqual(usd_rate['previous'], 75.0)
        self.assertEqual(usd_rate['change'], 0.5)
    
    @patch.object(CBRDataProvider, '_make_request_with_retry')
    def test_get_currency_rates_historical(self, mock_request):
        """Test historical currency rates retrieval"""
        mock_data = {
            'Date': '2024-01-10T11:30:00+03:00',
            'Valute': {
                'USD': {
                    'Name': 'Доллар США',
                    'Nominal': 1,
                    'Value': 74.8,
                    'Previous': 74.5
                }
            }
        }
        mock_request.return_value = mock_data
        
        rates = self.provider.get_currency_rates('2024-01-10')
        
        self.assertEqual(rates['source'], 'CBR')
        self.assertIn('USD', rates['rates'])
        self.assertEqual(rates['rates']['USD']['value'], 74.8)
    
    @patch.object(CBRDataProvider, '_make_request_with_retry')
    def test_get_currency_rates_invalid_date(self, mock_request):
        """Test currency rates with invalid date format"""
        rates = self.provider.get_currency_rates('invalid-date')
        self.assertEqual(rates, {})
        mock_request.assert_not_called()
    
    @patch.object(CBRDataProvider, '_make_request_with_retry')
    def test_get_currency_rates_empty_response(self, mock_request):
        """Test currency rates with empty response"""
        mock_request.return_value = None
        
        rates = self.provider.get_currency_rates()
        self.assertEqual(rates, {})
    
    @patch.object(CBRDataProvider, 'get_currency_rates')
    def test_get_currency_dynamics_success(self, mock_get_rates):
        """Test successful currency dynamics calculation"""
        # Mock responses for different dates (using weekdays only)
        def mock_rates_side_effect(date):
            if date in ['2024-01-15', '2024-01-16', '2024-01-17']:  # Monday, Tuesday, Wednesday
                if date == '2024-01-15':
                    return {
                        'rates': {
                            'USD': {'value': 74.0, 'nominal': 1}
                        }
                    }
                elif date == '2024-01-16':
                    return {
                        'rates': {
                            'USD': {'value': 75.0, 'nominal': 1}
                        }
                    }
                elif date == '2024-01-17':
                    return {
                        'rates': {
                            'USD': {'value': 76.0, 'nominal': 1}
                        }
                    }
            return {}
        
        mock_get_rates.side_effect = mock_rates_side_effect
        
        # Mock datetime to control the date range
        with patch('cbr_provider.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 17)  # Wednesday
            mock_datetime.strptime = datetime.strptime
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            with patch('time.sleep'):  # Skip sleep delays
                dynamics = self.provider.get_currency_dynamics('USD', days=3)
        
        self.assertEqual(dynamics['currency'], 'USD')
        self.assertEqual(dynamics['first_value'], 74.0)
        self.assertEqual(dynamics['last_value'], 76.0)
        self.assertEqual(dynamics['change_absolute'], 2.0)
        self.assertAlmostEqual(dynamics['change_percent'], 2.7, places=1)
        self.assertGreater(dynamics['volatility_percent'], 0)
    
    def test_get_currency_dynamics_unsupported_currency(self):
        """Test currency dynamics with unsupported currency"""
        dynamics = self.provider.get_currency_dynamics('XXX')
        self.assertEqual(dynamics, {})
    
    @patch.object(CBRDataProvider, 'get_currency_rates')
    def test_get_currency_dynamics_no_data(self, mock_get_rates):
        """Test currency dynamics with no data available"""
        mock_get_rates.return_value = {}
        
        with patch('time.sleep'):  # Skip sleep delays
            dynamics = self.provider.get_currency_dynamics('USD', days=1)
        
        self.assertEqual(dynamics, {})
    
    def test_get_key_rate_info_not_available(self):
        """Test key rate info returns not available message"""
        key_rate_info = self.provider.get_key_rate_info()
        
        self.assertIsNone(key_rate_info['key_rate'])
        self.assertIn('note', key_rate_info)
        self.assertIn('недоступна', key_rate_info['note'])
        self.assertEqual(key_rate_info['source'], 'CBR')
    
    @patch.object(CBRDataProvider, 'get_currency_rates')
    @patch.object(CBRDataProvider, 'get_key_rate_info')
    @patch.object(CBRDataProvider, 'get_currency_dynamics')
    def test_get_macro_indicators(self, mock_dynamics, mock_key_rate, mock_rates):
        """Test macro indicators aggregation"""
        mock_rates.return_value = {'rates': {'USD': {'value': 75.0}}}
        mock_key_rate.return_value = {'key_rate': None}
        mock_dynamics.return_value = {
            'last_value': 75.0,
            'change_percent': 1.5,
            'volatility_percent': 2.0
        }
        
        indicators = self.provider.get_macro_indicators()
        
        self.assertIn('currency_rates', indicators)
        self.assertIn('key_rate', indicators)
        self.assertIn('major_currencies_dynamics', indicators)
        self.assertEqual(indicators['source'], 'CBR')
        
        # Check that major currencies are included
        self.assertIn('USD', indicators['major_currencies_dynamics'])
    
    def test_get_source_info(self):
        """Test source info retrieval"""
        info = self.provider.get_source_info()
        
        self.assertEqual(info['name'], 'cbr')
        self.assertEqual(info['display_name'], 'Центральный банк РФ')
        self.assertEqual(info['type'], 'official_api')
        self.assertEqual(info['data_quality'], 'high')
        self.assertFalse(info['real_time'])
        self.assertEqual(info['data_scope'], 'macro_economic')
        self.assertIn('supported_currencies', info)
        self.assertIn('limitations', info)
        self.assertIsInstance(info['limitations'], list)
        self.assertGreater(len(info['limitations']), 0)
    
    @patch('cbr_provider.requests.Session.get')
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
    
    @patch('cbr_provider.requests.Session.get')
    def test_make_request_with_retry_timeout_then_success(self, mock_get):
        """Test request retry after timeout"""
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
    
    @patch('cbr_provider.requests.Session.get')
    def test_make_request_with_retry_all_attempts_fail(self, mock_get):
        """Test request failure after all retry attempts"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)  # Default retry_attempts
    
    def test_supported_currencies_completeness(self):
        """Test that supported currencies include expected ones"""
        expected_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CNY']
        
        for currency in expected_currencies:
            self.assertIn(currency, self.provider.supported_currencies)
            self.assertIsInstance(self.provider.supported_currencies[currency], str)
            self.assertGreater(len(self.provider.supported_currencies[currency]), 0)


if __name__ == '__main__':
    unittest.main()