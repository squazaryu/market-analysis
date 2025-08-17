"""
Unit tests for ETF Data Collector with Fallback
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

from etf_data_collector import ETFDataCollectorWithFallback
from fallback_system import FallbackResult, AllProvidersUnavailableError


class TestETFDataCollectorWithFallback(unittest.TestCase):
    """Test ETF Data Collector functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('etf_data_collector.DataProviderManager'):
            self.collector = ETFDataCollectorWithFallback()
    
    def test_initialization(self):
        """Test collector initialization"""
        self.assertIsNotNone(self.collector.fallback_manager)
        self.assertIsNotNone(self.collector.known_etfs)
        self.assertGreater(len(self.collector.known_etfs), 0)
    
    @patch('etf_data_collector.DataProviderManager')
    def test_collect_etf_data_success(self, mock_manager_class):
        """Test successful ETF data collection for single ticker"""
        # Setup mock fallback manager
        mock_manager = Mock()
        mock_result = FallbackResult(
            data={
                'ticker': 'SBMX',
                'current_price': 100.5,
                'return_annualized': 15.2,
                'volatility': 25.0,
                'avg_daily_volume': 50000
            },
            source='moex',
            quality_score=0.9,
            is_cached=False,
            fallback_level=0,
            warnings=[],
            metadata={}
        )
        mock_manager.get_etf_data_with_fallback.return_value = mock_result
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.collect_etf_data('SBMX')
        
        self.assertEqual(result['ticker'], 'SBMX')
        self.assertEqual(result['current_price'], 100.5)
        self.assertEqual(result['data_source'], 'moex')
        self.assertEqual(result['data_quality_score'], 0.9)
        self.assertIn('name', result)  # Should include metadata
        
        mock_manager.get_etf_data_with_fallback.assert_called_once_with('SBMX')
    
    @patch('etf_data_collector.DataProviderManager')
    def test_collect_etf_data_failure(self, mock_manager_class):
        """Test ETF data collection when all providers fail"""
        # Setup mock fallback manager to raise exception
        mock_manager = Mock()
        mock_manager.get_etf_data_with_fallback.side_effect = AllProvidersUnavailableError({
            'moex': Exception('MOEX failed'),
            'yahoo_finance': Exception('Yahoo failed')
        })
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.collect_etf_data('SBMX')
        
        self.assertEqual(result['ticker'], 'SBMX')
        self.assertEqual(result['data_source'], 'fallback_metadata')
        self.assertEqual(result['fallback_level'], 99)
        self.assertIsNone(result['current_price'])
        self.assertIn('warnings', result)
    
    @patch('etf_data_collector.DataProviderManager')
    def test_get_etf_list_success(self, mock_manager_class):
        """Test successful ETF list retrieval"""
        # Setup mock fallback manager
        mock_manager = Mock()
        mock_result = FallbackResult(
            data={
                'securities': [
                    {'ticker': 'SBMX', 'name': 'Сбер - Мосбиржа'},
                    {'ticker': 'VTBX', 'name': 'ВТБ - Мосбиржа'}
                ],
                'count': 2
            },
            source='moex',
            quality_score=1.0,
            is_cached=False,
            fallback_level=0,
            warnings=[],
            metadata={}
        )
        mock_manager.get_etf_list_with_fallback.return_value = mock_result
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.get_etf_list()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['ticker'], 'SBMX')
        # Should be enriched with metadata from known_etfs
        self.assertIn('management_company', result[0])
        
        mock_manager.get_etf_list_with_fallback.assert_called_once()
    
    @patch('etf_data_collector.DataProviderManager')
    def test_get_etf_list_failure(self, mock_manager_class):
        """Test ETF list retrieval when providers fail"""
        # Setup mock fallback manager to raise exception
        mock_manager = Mock()
        mock_manager.get_etf_list_with_fallback.side_effect = Exception('All providers failed')
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.get_etf_list()
        
        # Should return fallback list from known_etfs
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]['source'], 'fallback_knowledge_base')
        self.assertIn('ticker', result[0])
        self.assertIn('name', result[0])
    
    @patch('etf_data_collector.DataProviderManager')
    def test_get_macro_data_success(self, mock_manager_class):
        """Test successful macro data retrieval"""
        # Setup mock fallback manager
        mock_manager = Mock()
        mock_result = FallbackResult(
            data={
                'currency_rates': {'USD': {'value': 75.5}},
                'key_rate': None
            },
            source='cbr',
            quality_score=0.9,
            is_cached=False,
            fallback_level=0,
            warnings=[],
            metadata={}
        )
        mock_manager.get_macro_data_with_fallback.return_value = mock_result
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.get_macro_data()
        
        self.assertIn('currency_rates', result)
        self.assertEqual(result['data_source'], 'cbr')
        self.assertEqual(result['quality_score'], 0.9)
        
        mock_manager.get_macro_data_with_fallback.assert_called_once()
    
    @patch('etf_data_collector.DataProviderManager')
    def test_get_provider_status(self, mock_manager_class):
        """Test provider status retrieval"""
        # Setup mock fallback manager
        mock_manager = Mock()
        mock_status = {
            'providers': [
                {'name': 'moex', 'status': 'active'},
                {'name': 'yahoo_finance', 'status': 'degraded'}
            ],
            'total_providers': 2,
            'active_providers': 1
        }
        mock_manager.get_provider_status.return_value = mock_status
        mock_manager_class.return_value = mock_manager
        
        collector = ETFDataCollectorWithFallback()
        result = collector.get_provider_status()
        
        self.assertEqual(result, mock_status)
        mock_manager.get_provider_status.assert_called_once()
    
    def test_merge_data_with_metadata(self):
        """Test merging API data with metadata"""
        api_data = {
            'current_price': 100.5,
            'return_annualized': 15.2,
            'volatility': 25.0
        }
        
        metadata = {
            'name': 'Сбер - Мосбиржа',
            'management_company': 'Сбер Управление Активами',
            'category': 'Российские акции',
            'expense_ratio': 0.95
        }
        
        result = self.collector._merge_data_with_metadata('SBMX', api_data, metadata)
        
        self.assertEqual(result['ticker'], 'SBMX')
        self.assertEqual(result['name'], 'Сбер - Мосбиржа')
        self.assertEqual(result['current_price'], 100.5)
        self.assertEqual(result['annual_return'], 15.2)
        self.assertEqual(result['expense_ratio'], 0.95)
    
    def test_create_fallback_record(self):
        """Test creating fallback record with minimal data"""
        metadata = {
            'name': 'Сбер - Мосбиржа',
            'management_company': 'Сбер Управление Активами',
            'expense_ratio': 0.95
        }
        
        result = self.collector._create_fallback_record('SBMX', metadata)
        
        self.assertEqual(result['ticker'], 'SBMX')
        self.assertEqual(result['name'], 'Сбер - Мосбиржа')
        self.assertEqual(result['data_source'], 'fallback_metadata')
        self.assertEqual(result['fallback_level'], 99)
        self.assertIsNone(result['current_price'])
        self.assertIn('warnings', result)
    
    def test_calculate_market_shares(self):
        """Test market share calculation"""
        # Create test DataFrame
        test_data = [
            {
                'ticker': 'SBMX',
                'management_company': 'Сбер Управление Активами',
                'daily_value_rub': 1000000
            },
            {
                'ticker': 'VTBX',
                'management_company': 'ВТБ Капитал',
                'daily_value_rub': 500000
            },
            {
                'ticker': 'FXRU',
                'management_company': 'FinEx',
                'daily_value_rub': 750000
            }
        ]
        
        df = pd.DataFrame(test_data)
        result_df = self.collector._calculate_market_shares(df)
        
        self.assertIn('market_share_percent', result_df.columns)
        
        # Check that market shares sum to 100% (approximately)
        total_share = result_df['market_share_percent'].sum()
        self.assertAlmostEqual(total_share, 100.0, places=0)  # Allow for rounding differences
        
        # Check individual shares
        sber_share = result_df[result_df['ticker'] == 'SBMX']['market_share_percent'].iloc[0]
        self.assertGreater(sber_share, 40)  # Should have largest share
    
    def test_calculate_market_shares_no_volume_data(self):
        """Test market share calculation with no volume data"""
        test_data = [
            {
                'ticker': 'SBMX',
                'management_company': 'Сбер Управление Активами',
                'daily_value_rub': None
            }
        ]
        
        df = pd.DataFrame(test_data)
        result_df = self.collector._calculate_market_shares(df)
        
        self.assertIn('market_share_percent', result_df.columns)
        self.assertTrue(result_df['market_share_percent'].isna().all())
    
    def test_analyze_performance(self):
        """Test performance analysis"""
        test_data = [
            {'ticker': 'SBMX', 'name': 'Сбер - Мосбиржа', 'annual_return': 15.2},
            {'ticker': 'VTBX', 'name': 'ВТБ - Мосбиржа', 'annual_return': 12.5},
            {'ticker': 'FXRU', 'name': 'FinEx Россия', 'annual_return': -5.0}
        ]
        
        df = pd.DataFrame(test_data)
        result = self.collector._analyze_performance(df)
        
        self.assertIn('average_return', result)
        self.assertIn('best_performer', result)
        self.assertIn('worst_performer', result)
        
        self.assertEqual(result['best_performer']['ticker'], 'SBMX')
        self.assertEqual(result['worst_performer']['ticker'], 'FXRU')
        self.assertEqual(result['positive_returns_count'], 2)
        self.assertEqual(result['negative_returns_count'], 1)
    
    def test_analyze_performance_no_data(self):
        """Test performance analysis with no return data"""
        test_data = [
            {'ticker': 'SBMX', 'name': 'Сбер - Мосбиржа', 'annual_return': None}
        ]
        
        df = pd.DataFrame(test_data)
        result = self.collector._analyze_performance(df)
        
        self.assertIn('note', result)
        self.assertIn('недоступны', result['note'])
    
    @patch('etf_data_collector.DataProviderManager')
    def test_collect_all_etf_data_mixed_results(self, mock_manager_class):
        """Test collecting all ETF data with mixed success/failure results"""
        # Setup mock fallback manager
        mock_manager = Mock()
        
        def mock_get_etf_data(ticker):
            if ticker == 'SBMX':
                return FallbackResult(
                    data={'ticker': ticker, 'current_price': 100.5},
                    source='moex',
                    quality_score=0.9,
                    is_cached=False,
                    fallback_level=0,
                    warnings=[],
                    metadata={}
                )
            else:
                raise AllProvidersUnavailableError({'moex': Exception('Failed')})
        
        mock_manager.get_etf_data_with_fallback.side_effect = mock_get_etf_data
        mock_manager_class.return_value = mock_manager
        
        # Patch time.sleep to speed up test
        with patch('time.sleep'):
            collector = ETFDataCollectorWithFallback()
            result_df = collector.collect_all_etf_data()
        
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertGreater(len(result_df), 0)
        
        # Check that we have both successful and failed collections
        successful = result_df[result_df['data_source'] != 'fallback_metadata']
        failed = result_df[result_df['data_source'] == 'fallback_metadata']
        
        self.assertGreater(len(successful), 0)
        self.assertGreater(len(failed), 0)


if __name__ == '__main__':
    unittest.main()