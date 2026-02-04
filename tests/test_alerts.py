"""
Unit tests for Alert Strategy classes
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from src.alerts import (
    AlertStrategy,
    FatalErrorsPerMinuteAlert,
    FatalErrorsPerBundlePerHourAlert,
    AlertEngine
)


class TestFatalErrorsPerMinuteAlert:
    """Test suite for FatalErrorsPerMinuteAlert"""
    
    @pytest.fixture
    def sample_data_with_spike(self):
        """Create sample data with a fatal error spike"""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        dates = [base_time + timedelta(seconds=i*3) for i in range(15)]
        
        data = {
            'date': dates,
            'severity': ['fatal'] * 15,
            'bundle_id': ['com.example.app'] * 15,
            'error_code': [1001] * 15
        }
        
        return pd.DataFrame(data)
    
    @pytest.fixture
    def sample_data_no_spike(self):
        """Create sample data without a spike"""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        dates = [base_time + timedelta(minutes=i*2) for i in range(5)]
        
        data = {
            'date': dates,
            'severity': ['fatal'] * 5,
            'bundle_id': ['com.example.app'] * 5,
            'error_code': [1001] * 5
        }
        
        return pd.DataFrame(data)
    
    def test_alert_initialization(self):
        """Test alert can be initialized"""
        alert = FatalErrorsPerMinuteAlert(threshold=10, window='min')
        assert alert.threshold == 10
        assert alert.window == 'min'
        assert alert.rule_name is not None
    
    def test_detect_spike(self, sample_data_with_spike):
        """Test detection of fatal error spike"""
        alert = FatalErrorsPerMinuteAlert(threshold=10, window='min')
        results = alert.check(sample_data_with_spike)
        
        assert len(results) > 0
        assert all('fatal_count' in r for r in results)
        assert all(r['fatal_count'] > 10 for r in results)
    
    def test_no_false_positives(self, sample_data_no_spike):
        """Test no alerts when threshold not exceeded"""
        alert = FatalErrorsPerMinuteAlert(threshold=10, window='min')
        results = alert.check(sample_data_no_spike)
        
        assert len(results) == 0
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        alert = FatalErrorsPerMinuteAlert(threshold=10, window='min')
        empty_df = pd.DataFrame(columns=['date', 'severity', 'bundle_id'])
        
        results = alert.check(empty_df)
        assert results == []


class TestFatalErrorsPerBundlePerHourAlert:
    """Test suite for FatalErrorsPerBundlePerHourAlert"""
    
    @pytest.fixture
    def sample_data_multiple_bundles(self):
        """Create sample data with multiple bundles"""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        bundle1_dates = [base_time + timedelta(minutes=i*3) for i in range(15)]
        bundle1_data = {
            'date': bundle1_dates,
            'severity': ['fatal'] * 15,
            'bundle_id': ['com.example.app1'] * 15,
            'error_code': [1001] * 15
        }
        
        bundle2_dates = [base_time + timedelta(minutes=i*10) for i in range(5)]
        bundle2_data = {
            'date': bundle2_dates,
            'severity': ['fatal'] * 5,
            'bundle_id': ['com.example.app2'] * 5,
            'error_code': [1002] * 5
        }
        
        df1 = pd.DataFrame(bundle1_data)
        df2 = pd.DataFrame(bundle2_data)
        
        return pd.concat([df1, df2], ignore_index=True)
    
    def test_alert_initialization(self):
        """Test alert can be initialized"""
        alert = FatalErrorsPerBundlePerHourAlert(threshold=10, window='h')
        assert alert.threshold == 10
        assert alert.window == 'h'
        assert alert.rule_name is not None
    
    def test_detect_bundle_spike(self, sample_data_multiple_bundles):
        """Test detection of bundle-specific spikes"""
        alert = FatalErrorsPerBundlePerHourAlert(threshold=10, window='h')
        results = alert.check(sample_data_multiple_bundles)
        
        assert len(results) > 0
        
        assert all('bundle_id' in r for r in results)
        
        bundle_ids = [r['bundle_id'] for r in results]
        assert 'com.example.app1' in bundle_ids
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        alert = FatalErrorsPerBundlePerHourAlert(threshold=10, window='h')
        empty_df = pd.DataFrame(columns=['date', 'severity', 'bundle_id'])
        
        results = alert.check(empty_df)
        assert results == []


class TestAlertEngine:
    """Test suite for AlertEngine orchestrator"""
    
    def test_engine_initialization(self):
        """Test engine can be initialized"""
        engine = AlertEngine()
        assert engine.get_strategy_count() == 0
    
    def test_add_strategy(self):
        """Test adding strategies to engine"""
        engine = AlertEngine()
        
        engine.add_strategy(FatalErrorsPerMinuteAlert())
        assert engine.get_strategy_count() == 1
        
        engine.add_strategy(FatalErrorsPerBundlePerHourAlert())
        assert engine.get_strategy_count() == 2
    
    def test_run_all_checks(self):
        """Test running all strategies"""
        engine = AlertEngine()
        engine.add_strategy(FatalErrorsPerMinuteAlert(threshold=10))
        engine.add_strategy(FatalErrorsPerBundlePerHourAlert(threshold=10))
        
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        dates = [base_time + timedelta(seconds=i*2) for i in range(20)]
        
        df = pd.DataFrame({
            'date': dates,
            'severity': ['fatal'] * 20,
            'bundle_id': ['com.example.app'] * 20,
            'error_code': [1001] * 20
        })
        
        results = engine.run_all_checks(df)
        
        assert isinstance(results, list)
