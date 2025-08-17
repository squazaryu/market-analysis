"""
Unit tests for fallback system core interfaces and base classes
"""

import unittest
from datetime import datetime, timedelta
from typing import Dict
from fallback_system import (
    DataSourceStatus, FallbackResult, FallbackConfig, ProviderMetrics,
    DataProvider, FallbackSystemError, AllProvidersUnavailableError,
    DataQualityError, CacheExpiredError, DEFAULT_FALLBACK_PROVIDERS
)


class MockDataProvider(DataProvider):
    """Mock implementation of DataProvider for testing"""
    
    def __init__(self, name: str, should_fail: bool = False, priority: int = 1, config: Dict = None):
        super().__init__(name, priority=priority, config=config)
        self.should_fail = should_fail
    
    def get_securities_list(self):
        if self.should_fail:
            raise Exception("Mock provider failure")
        return [{'ticker': 'TEST', 'name': 'Test Security'}]
    
    def get_market_data(self, ticker: str):
        if self.should_fail:
            raise Exception("Mock provider failure")
        return {'ticker': ticker, 'price': 100.0, 'currency': 'RUB'}
    
    def get_historical_data(self, ticker: str, days: int = 365):
        if self.should_fail:
            raise Exception("Mock provider failure")
        return {'ticker': ticker, 'return_1y': 15.5, 'volatility': 25.0}
    
    def get_trading_volume_data(self, ticker: str):
        if self.should_fail:
            raise Exception("Mock provider failure")
        return {'ticker': ticker, 'avg_volume': 1000000}
    
    def health_check(self):
        return DataSourceStatus.UNAVAILABLE if self.should_fail else DataSourceStatus.ACTIVE
    
    def get_source_info(self):
        return {'name': self.name, 'type': 'mock', 'version': '1.0'}


class TestDataSourceStatus(unittest.TestCase):
    """Test DataSourceStatus enum"""
    
    def test_enum_values(self):
        """Test that all expected enum values exist"""
        self.assertEqual(DataSourceStatus.ACTIVE.value, "active")
        self.assertEqual(DataSourceStatus.DEGRADED.value, "degraded")
        self.assertEqual(DataSourceStatus.UNAVAILABLE.value, "unavailable")
        self.assertEqual(DataSourceStatus.UNKNOWN.value, "unknown")


class TestFallbackResult(unittest.TestCase):
    """Test FallbackResult dataclass"""
    
    def test_creation_with_defaults(self):
        """Test creating FallbackResult with default values"""
        result = FallbackResult(
            data={'test': 'data'},
            source='test_source',
            quality_score=0.8,
            is_cached=False
        )
        
        self.assertEqual(result.data, {'test': 'data'})
        self.assertEqual(result.source, 'test_source')
        self.assertEqual(result.quality_score, 0.8)
        self.assertFalse(result.is_cached)
        self.assertIsNone(result.cache_age_hours)
        self.assertEqual(result.fallback_level, 0)
        self.assertEqual(result.warnings, [])
        self.assertEqual(result.metadata, {})
        self.assertIsInstance(result.timestamp, datetime)
    
    def test_creation_with_all_fields(self):
        """Test creating FallbackResult with all fields specified"""
        warnings = ['Warning 1', 'Warning 2']
        metadata = {'source_type': 'api', 'response_time': 150}
        timestamp = datetime.now()
        
        result = FallbackResult(
            data={'ticker': 'SBER'},
            source='moex',
            quality_score=0.95,
            is_cached=True,
            cache_age_hours=2.5,
            fallback_level=1,
            warnings=warnings,
            metadata=metadata,
            timestamp=timestamp
        )
        
        self.assertEqual(result.cache_age_hours, 2.5)
        self.assertEqual(result.fallback_level, 1)
        self.assertEqual(result.warnings, warnings)
        self.assertEqual(result.metadata, metadata)
        self.assertEqual(result.timestamp, timestamp)


class TestFallbackConfig(unittest.TestCase):
    """Test FallbackConfig dataclass"""
    
    def test_default_values(self):
        """Test FallbackConfig with default values"""
        config = FallbackConfig()
        
        self.assertEqual(config.providers, [])
        self.assertEqual(config.health_check_interval, 300)
        self.assertEqual(config.cache_ttl_hours, 24)
        self.assertEqual(config.max_cache_age_hours, 168)
        self.assertEqual(config.quality_threshold, 0.7)
        
        # Test default notification settings
        expected_notifications = {
            'email_alerts': False,
            'telegram_alerts': False,
            'log_level': 'WARNING'
        }
        self.assertEqual(config.notification_settings, expected_notifications)
    
    def test_custom_values(self):
        """Test FallbackConfig with custom values"""
        providers = [{'name': 'test', 'priority': 1}]
        notifications = {'email_alerts': True, 'log_level': 'INFO'}
        
        config = FallbackConfig(
            providers=providers,
            health_check_interval=600,
            cache_ttl_hours=48,
            max_cache_age_hours=336,
            quality_threshold=0.8,
            notification_settings=notifications
        )
        
        self.assertEqual(config.providers, providers)
        self.assertEqual(config.health_check_interval, 600)
        self.assertEqual(config.cache_ttl_hours, 48)
        self.assertEqual(config.max_cache_age_hours, 336)
        self.assertEqual(config.quality_threshold, 0.8)
        self.assertEqual(config.notification_settings, notifications)


class TestProviderMetrics(unittest.TestCase):
    """Test ProviderMetrics dataclass"""
    
    def test_creation(self):
        """Test creating ProviderMetrics"""
        timestamp = datetime.now()
        metrics = ProviderMetrics(
            status=DataSourceStatus.ACTIVE,
            response_time_ms=150.5,
            success_rate_24h=0.98,
            last_successful_request=timestamp,
            error_count_1h=2,
            total_requests=1000,
            cache_hit_rate=0.75
        )
        
        self.assertEqual(metrics.status, DataSourceStatus.ACTIVE)
        self.assertEqual(metrics.response_time_ms, 150.5)
        self.assertEqual(metrics.success_rate_24h, 0.98)
        self.assertEqual(metrics.last_successful_request, timestamp)
        self.assertEqual(metrics.error_count_1h, 2)
        self.assertEqual(metrics.total_requests, 1000)
        self.assertEqual(metrics.cache_hit_rate, 0.75)


class TestDataProvider(unittest.TestCase):
    """Test DataProvider abstract base class"""
    
    def setUp(self):
        self.provider = MockDataProvider("test_provider")
    
    def test_initialization(self):
        """Test DataProvider initialization"""
        config = {'timeout': 10, 'retries': 3}
        provider = MockDataProvider("test", config=config)
        
        self.assertEqual(provider.name, "test")
        self.assertEqual(provider.priority, 1)
        self.assertEqual(provider.config, config)
        self.assertTrue(provider.enabled)
        self.assertIsInstance(provider.metrics, ProviderMetrics)
    
    def test_is_available_when_active(self):
        """Test is_available returns True for active provider"""
        self.provider.should_fail = False
        self.assertTrue(self.provider.is_available())
    
    def test_is_available_when_unavailable(self):
        """Test is_available returns False for unavailable provider"""
        self.provider.should_fail = True
        self.assertFalse(self.provider.is_available())
    
    def test_is_available_when_disabled(self):
        """Test is_available returns False for disabled provider"""
        self.provider.enabled = False
        self.assertFalse(self.provider.is_available())
    
    def test_update_metrics_success(self):
        """Test updating metrics for successful request"""
        initial_requests = self.provider.metrics.total_requests
        
        self.provider.update_metrics(response_time=150.0, success=True)
        
        self.assertEqual(self.provider.metrics.response_time_ms, 150.0)
        self.assertEqual(self.provider.metrics.total_requests, initial_requests + 1)
        self.assertIsNotNone(self.provider.metrics.last_successful_request)
    
    def test_update_metrics_failure(self):
        """Test updating metrics for failed request"""
        initial_requests = self.provider.metrics.total_requests
        initial_errors = self.provider.metrics.error_count_1h
        
        self.provider.update_metrics(response_time=5000.0, success=False)
        
        self.assertEqual(self.provider.metrics.response_time_ms, 5000.0)
        self.assertEqual(self.provider.metrics.total_requests, initial_requests + 1)
        self.assertEqual(self.provider.metrics.error_count_1h, initial_errors + 1)
    
    def test_abstract_methods_implemented(self):
        """Test that all abstract methods are implemented in mock"""
        # These should not raise NotImplementedError
        self.provider.get_securities_list()
        self.provider.get_market_data("TEST")
        self.provider.get_historical_data("TEST")
        self.provider.get_trading_volume_data("TEST")
        self.provider.health_check()
        self.provider.get_source_info()


class TestFallbackSystemExceptions(unittest.TestCase):
    """Test custom exceptions"""
    
    def test_fallback_system_error(self):
        """Test base FallbackSystemError"""
        error = FallbackSystemError("Test error")
        self.assertEqual(str(error), "Test error")
        self.assertIsInstance(error, Exception)
    
    def test_all_providers_unavailable_error(self):
        """Test AllProvidersUnavailableError"""
        provider_errors = {
            'moex': Exception("MOEX failed"),
            'yahoo': Exception("Yahoo failed")
        }
        
        error = AllProvidersUnavailableError(provider_errors)
        self.assertEqual(error.provider_errors, provider_errors)
        self.assertIn("moex", str(error))
        self.assertIn("yahoo", str(error))
    
    def test_data_quality_error(self):
        """Test DataQualityError"""
        error = DataQualityError(quality_score=0.5, threshold=0.7)
        self.assertEqual(error.quality_score, 0.5)
        self.assertEqual(error.threshold, 0.7)
        self.assertIn("0.50", str(error))
        self.assertIn("0.70", str(error))
    
    def test_cache_expired_error(self):
        """Test CacheExpiredError"""
        error = CacheExpiredError(cache_age_hours=48.5, max_age_hours=24.0)
        self.assertEqual(error.cache_age_hours, 48.5)
        self.assertEqual(error.max_age_hours, 24.0)
        self.assertIn("48.5", str(error))
        self.assertIn("24.0", str(error))


class TestDefaultConfiguration(unittest.TestCase):
    """Test default fallback providers configuration"""
    
    def test_default_providers_structure(self):
        """Test that default providers have required structure"""
        self.assertIsInstance(DEFAULT_FALLBACK_PROVIDERS, list)
        self.assertGreater(len(DEFAULT_FALLBACK_PROVIDERS), 0)
        
        for provider in DEFAULT_FALLBACK_PROVIDERS:
            self.assertIn('name', provider)
            self.assertIn('class', provider)
            self.assertIn('priority', provider)
            self.assertIn('enabled', provider)
            self.assertIn('config', provider)
            
            # Check types
            self.assertIsInstance(provider['name'], str)
            self.assertIsInstance(provider['class'], str)
            self.assertIsInstance(provider['priority'], int)
            self.assertIsInstance(provider['enabled'], bool)
            self.assertIsInstance(provider['config'], dict)
    
    def test_provider_priorities(self):
        """Test that providers have different priorities"""
        priorities = [p['priority'] for p in DEFAULT_FALLBACK_PROVIDERS]
        self.assertEqual(len(priorities), len(set(priorities)), 
                        "All providers should have unique priorities")
    
    def test_moex_provider_config(self):
        """Test MOEX provider configuration"""
        moex_provider = next(p for p in DEFAULT_FALLBACK_PROVIDERS if p['name'] == 'moex')
        
        self.assertEqual(moex_provider['class'], 'MOEXDataProvider')
        self.assertEqual(moex_provider['priority'], 1)  # Highest priority
        self.assertTrue(moex_provider['enabled'])
        
        config = moex_provider['config']
        self.assertIn('base_url', config)
        self.assertIn('timeout', config)
        self.assertIn('retry_attempts', config)


if __name__ == '__main__':
    unittest.main()