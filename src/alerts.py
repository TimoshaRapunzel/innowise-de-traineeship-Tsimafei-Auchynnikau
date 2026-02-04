"""
Alert Strategy Module - Extensible Alerting System

This module implements the Strategy Pattern for alert detection.
All rules use vectorized Pandas operations for maximum performance.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict
from datetime import timedelta


class AlertStrategy(ABC):
    """
    Abstract base class for alert detection strategies.
    
    All concrete implementations must use vectorized Pandas operations
    to handle high-throughput data (100M records/day).
    """
    
    @abstractmethod
    def check(self, df: pd.DataFrame) -> List[Dict]:
        """
        Check for alerts in the provided DataFrame.
        
        Args:
            df: DataFrame containing log data
            
        Returns:
            List of alert dictionaries with details about detected issues
        """
        pass
    
    @property
    @abstractmethod
    def rule_name(self) -> str:
        """Return the name of this alert rule."""
        pass


class FatalErrorsPerMinuteAlert(AlertStrategy):
    """
    Rule 2.1: Alert on >10 fatal errors in <1 minute.
    
    Implementation:
    - Uses time-based rolling window on datetime index
    - Fully vectorized with no row iteration
    - Efficient for large datasets
    """
    
    def __init__(self, threshold: int = 10, window: str = 'min'):
        """
        Initialize the alert rule.
        
        Args:
            threshold: Number of fatal errors to trigger alert
            window: Time window for rolling count (pandas offset string)
        """
        self.threshold = threshold
        self.window = window
    
    @property
    def rule_name(self) -> str:
        return f"Fatal Errors Per Minute (>{self.threshold} in {self.window})"
    
    def check(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect time windows with excessive fatal errors.
        
        Args:
            df: DataFrame with 'date' and 'severity' columns
            
        Returns:
            List of alerts with timestamp and count information
        """
        if df.empty:
            return []
        
        fatal_df = df[df['severity'] == 'fatal'].copy()
        
        if fatal_df.empty:
            return []
        
        if not pd.api.types.is_datetime64_any_dtype(fatal_df['date']):
            fatal_df['date'] = pd.to_datetime(fatal_df['date'])
        
        fatal_df = fatal_df.sort_values('date')
        
        fatal_df = fatal_df.set_index('date')
        
        rolling_counts = fatal_df.rolling(self.window).count()['severity']
        
        violations = rolling_counts[rolling_counts > self.threshold]
        
        alerts = []
        for timestamp, count in violations.items():
            alerts.append({
                'rule': self.rule_name,
                'timestamp': timestamp,
                'fatal_count': int(count),
                'threshold': self.threshold,
                'window': self.window,
                'severity': 'HIGH',
                'message': f"Detected {int(count)} fatal errors within {self.window} at {timestamp}"
            })
        
        return alerts


class FatalErrorsPerBundlePerHourAlert(AlertStrategy):
    """
    Rule 2.2: Alert on >10 fatal errors in <1 hour for a specific bundle_id.
    
    Implementation:
    - Groups by bundle_id then applies rolling window
    - Fully vectorized using groupby + rolling
    - Scales efficiently for many bundle_ids
    """
    
    def __init__(self, threshold: int = 10, window: str = 'h'):
        """
        Initialize the alert rule.
        
        Args:
            threshold: Number of fatal errors to trigger alert per bundle
            window: Time window for rolling count (pandas offset string)
        """
        self.threshold = threshold
        self.window = window
    
    @property
    def rule_name(self) -> str:
        return f"Fatal Errors Per Bundle Per Hour (>{self.threshold} in {self.window})"
    
    def check(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect bundle_ids with excessive fatal errors in time windows.
        
        Args:
            df: DataFrame with 'date', 'severity', and 'bundle_id' columns
            
        Returns:
            List of alerts with bundle_id, timestamp, and count information
        """
        if df.empty:
            return []
        
        fatal_df = df[df['severity'] == 'fatal'].copy()
        
        if fatal_df.empty:
            return []
        
        if not pd.api.types.is_datetime64_any_dtype(fatal_df['date']):
            fatal_df['date'] = pd.to_datetime(fatal_df['date'])
        
        fatal_df = fatal_df.sort_values(['bundle_id', 'date'])
        
        fatal_df = fatal_df.set_index('date')
        
        rolling_counts = (
            fatal_df.groupby('bundle_id')['severity']
            .rolling(self.window)
            .count()
        )
        
        violations = rolling_counts[rolling_counts > self.threshold]
        
        alerts = []
        for (bundle_id, timestamp), count in violations.items():
            alerts.append({
                'rule': self.rule_name,
                'bundle_id': bundle_id,
                'timestamp': timestamp,
                'fatal_count': int(count),
                'threshold': self.threshold,
                'window': self.window,
                'severity': 'HIGH',
                'message': f"Bundle {bundle_id}: {int(count)} fatal errors within {self.window} at {timestamp}"
            })
        
        return alerts


class AlertEngine:
    """
    Orchestrates multiple alert strategies.
    
    Usage:
        engine = AlertEngine()
        engine.add_strategy(FatalErrorsPerMinuteAlert())
        engine.add_strategy(FatalErrorsPerBundlePerHourAlert())
        alerts = engine.run_all_checks(df)
    """
    
    def __init__(self):
        """Initialize the alert engine."""
        self.strategies: List[AlertStrategy] = []
    
    def add_strategy(self, strategy: AlertStrategy) -> None:
        """
        Add an alert strategy to the engine.
        
        Args:
            strategy: AlertStrategy instance to add
        """
        self.strategies.append(strategy)
    
    def run_all_checks(self, df: pd.DataFrame) -> List[Dict]:
        """
        Run all registered alert strategies.
        
        Args:
            df: DataFrame to check for alerts
            
        Returns:
            Combined list of all alerts from all strategies
        """
        all_alerts = []
        
        for strategy in self.strategies:
            alerts = strategy.check(df)
            all_alerts.extend(alerts)
        
        return all_alerts
    
    def get_strategy_count(self) -> int:
        """Return the number of registered strategies."""
        return len(self.strategies)
