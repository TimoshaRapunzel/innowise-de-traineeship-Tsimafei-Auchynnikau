"""
LogLoader - Data Ingestion Module for Alert Project

This module handles loading and preprocessing of mobile application logs.
Optimized for high throughput (100M records/day) using memory-efficient data types.
"""

import pandas as pd
from pathlib import Path
from typing import Union, List


class LogLoader:
    """
    Loads CSV log files with optimized memory usage.
    
    Memory Optimization Strategy:
    - datetime64[ns]: Efficient storage for timestamps (8 bytes per value)
    - category dtype: For low-cardinality string columns (saves ~70% memory)
    - Explicit dtype specification: Prevents pandas from inferring inefficient types
    """
    
    REQUIRED_COLUMNS = [
        'error_code', 'error_message', 'severity', 'log_location', 'mode',
        'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id',
        'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id',
        'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date'
    ]
    
    CATEGORY_COLUMNS = ['severity', 'os', 'bundle_id', 'mode', 'country_code', 
                        'language', 'flow_type', 'test_mode']
    
    DATE_COLUMNS = ['date', 'sdk_date']
    
    def __init__(self):
        """Initialize the LogLoader."""
        self._dtype_spec = self._build_dtype_spec()
    
    def _build_dtype_spec(self) -> dict:
        """
        Build dtype specification for efficient loading.
        
        Returns:
            dict: Column name to dtype mapping
        """
        dtype_spec = {}
        
        for col in self.CATEGORY_COLUMNS:
            if col in self.REQUIRED_COLUMNS:
                dtype_spec[col] = 'category'
        
        return dtype_spec
    
    def load_csv(
        self, 
        file_path: Union[str, Path],
        validate_schema: bool = True
    ) -> pd.DataFrame:
        """
        Load a CSV log file with memory optimization.
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Log file not found: {file_path}")
        
        df = pd.read_csv(
            file_path,
            names=self.REQUIRED_COLUMNS,
            header=0,
            dtype=self._dtype_spec
        )
        
        if validate_schema:
            self._validate_schema(df)
        
        df = self._optimize_memory(df)
        
        return df
    
    def load_multiple_csv(
        self,
        file_paths: List[Union[str, Path]],
        validate_schema: bool = True
    ) -> pd.DataFrame:
        """
        Load and concatenate multiple CSV files.
        
        Args:
            file_paths: List of paths to CSV files
            validate_schema: Whether to validate column schema
            
        Returns:
            pd.DataFrame: Combined DataFrame
        """
        dfs = []
        
        for file_path in file_paths:
            df = self.load_csv(file_path, validate_schema=validate_schema)
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        return combined_df
    
    def _validate_schema(self, df: pd.DataFrame) -> None:
        """
        Validate that DataFrame has required columns.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If required columns are missing
        """
        missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
        
        if missing_cols:
            raise ValueError(
                f"Missing required columns: {sorted(missing_cols)}"
            )
    
    def _optimize_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply memory optimization techniques.
        
        Optimizations:
        1. Convert date columns to datetime64[ns]
        2. Convert low-cardinality strings to category
        3. Downcast numeric types where possible
        
        Args:
            df: DataFrame to optimize
            
        Returns:
            pd.DataFrame: Optimized DataFrame
        """
        for col in self.DATE_COLUMNS:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], unit='s')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        for col in self.CATEGORY_COLUMNS:
            if col in df.columns and df[col].dtype != 'category':
                df[col] = df[col].astype('category')
        
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        return df
    
    def get_memory_usage(self, df: pd.DataFrame) -> dict:
        """
        Get detailed memory usage statistics.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Memory usage statistics
        """
        memory_usage = df.memory_usage(deep=True)
        
        return {
            'total_mb': memory_usage.sum() / 1024**2,
            'per_column_mb': (memory_usage / 1024**2).to_dict(),
            'row_count': len(df),
            'column_count': len(df.columns)
        }
