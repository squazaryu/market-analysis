"""
Unit tests for Yahoo Finance Data Provider
"""

import unittest
from unittest.mock import Mock, patch
import requests
from datetime import datetime

from yahoo_finance_provider import YahooFinanceProvider
from fallback_system import DataSourceStatus


class TestYahooFinanceProvider(unittest.TestCase):
    """Test Yahoo Finance Data Provider functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.provider = YahooFinanceProvider()
    
    def test_initialization(self):
        """Test Yahoo Finance provider initialization"""
        self.assertEqual(self.provider.name, "yahoo_finance")
        self.assertEqual(self.provider.priority, 2)
        self.assertTrue(self.provider.enabled)
        self.assertIn('base_url', self.provider.config)
        self.assertEqual(self.provider.config['ticker_suffix'], '.ME')
    
    def test_custom_config_initialization(self):
        """Test Yahoo Finance provider with custom config"""
        custom_config = {
            'base_url': 'https://custom.yahoo.com/v8/finance',
            'timeout': 20,
            'ticker_suffix': '.MSK'
        }
        
        provider = YahooFinanceProvider(custom_config)
        self.assertEqual(provider.config, custom_config)
        self.assertEqual(provider.config['ticker_suffix'], '.MSK')
    
    def test_convert_ticker_known_mapping(self):
        """Test ticker conversion with known mapping"""
        self.assertEqual(self.provider._convert_ticker('SBER'), 'SBER.ME')
        self.assertEqual(self.provider._convert_ticker('SBMX'), 'SBMX.ME')
        self.assertEqual(self.provider._convert_ticker('GAZP'), 'GAZP.ME')
    
    def test_convert_ticker_already_has_suffix(self):
        """Test ticker conversion when ticker already has suffix"""
        self.assertEqual(self.provider._convert_ticker('SBER.ME'), 'SBER.ME')
        self.assertEqual(self.provider._convert_ticker('AAPL.US'), 'AAPL.US')
    
    def test_convert_ticker_unknown_ticker(self):
        """Test ticker conversion for unknown ticker"""
        self.assertEqual(self.provider._convert_ticker('UNKNOWN'), 'UNKNOWN.ME')
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_health_check_active(self, mock_get):
        """Test health check returns ACTIVE for fast response with valid data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'chart': {
                'result': [{'meta': {'regularMarketPrice': 100.0}}]
            }
        }
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 1.0]):  # 1000ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.ACTIVE)
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_health_check_degraded_slow_response(self, mock_get):
        """Test health check returns DEGRADED for slow response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'chart': {
                'result': [{'meta': {'regularMarketPrice': 100.0}}]
            }
        }
        mock_get.return_value = mock_response
        
        with patch('time.time', side_effect=[0, 5.0]):  # 5000ms response
            status = self.provider.health_check()
        
        self.assertEqual(status, DataSourceStatus.DEGRADED)
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_health_check_degraded_invalid_data(self, mock_get):
        """Test health check returns DEGRADED for invalid data structure"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'invalid': 'structure'}
        mock_get.return_value = mock_response
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.DEGRADED)
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_health_check_unavailable(self, mock_get):
        """Test health check returns UNAVAILABLE for connection error"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        status = self.provider.health_check()
        self.assertEqual(status, DataSourceStatus.UNAVAILABLE)
    
    def test_health_check_caching(self):
        """Test health check result is cached"""
        with patch('yahoo_finance_provider.requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'chart': {
                    'result': [{'meta': {'regularMarketPrice': 100.0}}]
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
    
    @patch.object(YahooFinanceProvider, 'get_market_data')
    def test_get_securities_list(self, mock_get_market_data):
        """Test securities list retrieval"""
        # Mock successful market data for some tickers
        def mock_market_data_side_effect(ticker):
            if ticker in ['SBER', 'SBMX']:
                return {
                    'shortName': f'{ticker} Short',
                    'longName': f'{ticker} Long Name',
                    'currency': 'RUB'
                }
            return {}
        
        mock_get_market_data.side_effect = mock_market_data_side_effect
        
        securities = self.provider.get_securities_list()
        
        # Should return securities for tickers that have market data
        self.assertGreater(len(securities), 0)
        
        # Check structure of returned securities
        for security in securities:
            self.assertIn('ticker', security)
            self.assertIn('yahoo_ticker', security)
            self.assertIn('source', security)
            self.assertEqual(security['source'], 'Yahoo Finance')
    
    @patch.object(YahooFinanceProvider, '_make_request_with_retry')
    def test_get_market_data_success(self, mock_request):
        """Test successful market data retrieval"""
        mock_data = {
            'chart': {
                'result': [{
                    'meta': {
                        'regularMarketPrice': 101.5,
                        'previousClose': 100.0,
                        'currency': 'RUB',
                        'exchangeName': 'MCX',
                        'marketState': 'REGULAR'
                    },
                    'timestamp': [1640995200],
                    'indicators': {
                        'quote': [{
                            'close': [100.0, 101.5],
                            'high': [102.0, 103.0],
                            'low': [99.0, 100.5],
                            'volume': [50000, 60000],
                            'open': [99.5, 101.0]
                        }]
                    }
                }]
            }
        }
        mock_request.return_value = mock_data
        
        market_data = self.provider.get_market_data('SBER')
        
        self.assertEqual(market_data['ticker'], 'SBER')
        self.assertEqual(market_data['yahoo_ticker'], 'SBER.ME')
        self.assertEqual(market_data['current_price'], 101.5)
        self.assertEqual(market_data['previous_close'], 100.0)
        self.assertEqual(market_data['currency'], 'RUB')
        self.assertEqual(market_data['source'], 'Yahoo Finance')
        self.assertIn('change', market_data)
        self.assertIn('change_percent', market_data)
        self.assertIn('data_quality', market_data)
    
    @patch.object(YahooFinanceProvider, '_make_request_with_retry')
    def test_get_market_data_empty_response(self, mock_request):
        """Test market data with empty response"""
        mock_request.return_value = None
        
        market_data = self.provider.get_market_data('SBER')
        self.assertEqual(market_data, {})
    
    @patch.object(YahooFinanceProvider, '_make_request_with_retry')
    def test_get_historical_data_success(self, mock_request):
        """Test successful historical data retrieval"""
        mock_data = {
            'chart': {
                'result': [{
                    'timestamp': [1640995200, 1641081600, 1641168000],
                    'indicators': {
                        'quote': [{
                            'close': [100.0, 105.0, 108.0]
                        }]
                    }
                }]
            }
        }
        mock_request.return_value = mock_data
        
        historical_data = self.provider.get_historical_data('SBER', days=30)
        
        self.assertEqual(historical_data['ticker'], 'SBER')
        self.assertEqual(historical_data['yahoo_ticker'], 'SBER.ME')
        self.assertIn('return_period', historical_data)
        self.assertIn('volatility', historical_data)
        self.assertIn('max_drawdown', historical_data)
        self.assertEqual(historical_data['data_points'], 3)
        self.assertEqual(historical_data['source'], 'Yahoo Finance')
        self.assertEqual(historical_data['data_delay_minutes'], 15)
    
    @patch.object(YahooFinanceProvider, '_make_request_with_retry')
    def test_get_trading_volume_data_success(self, mock_request):
        """Test successful trading volume data retrieval"""
        mock_data = {
            'chart': {
                'result': [{
                    'indicators': {
                        'quote': [{
                            'volume': [50000, 60000, 0, 70000],  # One day with no trading
                            'close': [100.0, 105.0, 105.0, 108.0]
                        }]
                    }
                }]
            }
        }
        mock_request.return_value = mock_data
        
        volume_data = self.provider.get_trading_volume_data('SBER')
        
        self.assertEqual(volume_data['ticker'], 'SBER')
        self.assertEqual(volume_data['yahoo_ticker'], 'SBER.ME')
        self.assertIn('avg_daily_volume', volume_data)
        self.assertIn('avg_daily_value_rub', volume_data)
        self.assertEqual(volume_data['trading_days'], 3)  # Days with volume > 0
        self.assertEqual(volume_data['total_days'], 4)
        self.assertAlmostEqual(volume_data['liquidity_score'], 0.75, places=2)
    
    def test_get_source_info(self):
        """Test source info retrieval"""
        info = self.provider.get_source_info()
        
        self.assertEqual(info['name'], 'yahoo_finance')
        self.assertEqual(info['display_name'], 'Yahoo Finance')
        self.assertEqual(info['type'], 'unofficial_api')
        self.assertEqual(info['data_quality'], 'medium')
        self.assertFalse(info['real_time'])
        self.assertEqual(info['data_delay_minutes'], 15)
        self.assertIn('limitations', info)
        self.assertIsInstance(info['limitations'], list)
        self.assertGreater(len(info['limitations']), 0)
    
    @patch('yahoo_finance_provider.requests.Session.get')
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
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_make_request_with_retry_404_not_found(self, mock_get):
        """Test request with 404 (ticker not found)"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        mock_get.assert_called_once()
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_make_request_with_retry_rate_limit(self, mock_get):
        """Test request with rate limit (429)"""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 2)  # Should retry for 429
    
    @patch('yahoo_finance_provider.requests.Session.get')
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
    
    @patch('yahoo_finance_provider.requests.Session.get')
    def test_make_request_with_retry_api_error(self, mock_get):
        """Test request with API error in response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"error": "API Error"}'
        mock_response.json.return_value = {"error": "API Error"}
        mock_get.return_value = mock_response
        
        result = self.provider._make_request_with_retry('http://test.com')
        
        self.assertIsNone(result)
    
    def test_assess_market_data_quality_high(self):
        """Test market data quality assessment - high quality"""
        data = {
            'current_price': 100.0,
            'change': 1.5,
            'volume': 50000,
            'currency': 'RUB',
            'timestamp': datetime.now().isoformat(),
            'high': 102.0,
            'low': 98.0
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 1.0)  # All checks pass
    
    def test_assess_market_data_quality_medium(self):
        """Test market data quality assessment - medium quality"""
        data = {
            'current_price': 100.0,
            'currency': 'RUB',
            'timestamp': datetime.now().isoformat()
            # Missing change, volume, high/low
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 0.5)  # 2.5 out of 5 checks pass
    
    def test_assess_market_data_quality_low(self):
        """Test market data quality assessment - low quality"""
        data = {
            'ticker': 'TEST'
            # Missing most required fields
        }
        
        quality = self.provider._assess_market_data_quality(data)
        self.assertEqual(quality, 0.0)  # No checks pass
    
    def test_ticker_mapping_completeness(self):
        """Test that ticker mapping includes expected tickers"""
        expected_tickers = ['SBER', 'GAZP', 'SBMX', 'VTBX', 'FXRU', 'TECH']
        
        for ticker in expected_tickers:
            self.assertIn(ticker, self.provider.ticker_mapping)
            self.assertTrue(self.provider.ticker_mapping[ticker].endswith('.ME'))


if __name__ == '__main__':
    unittest.main()