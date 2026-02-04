"""
Unit tests for LogLoader class
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

from src.loader import LogLoader


class TestLogLoader:
    """Test suite for LogLoader class"""
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data for testing"""
        return """error_code,error_message,severity,log_location,mode,model,graphics,session_id,sdkv,test_mode,flow_id,flow_type,sdk_date,publisher_id,game_id,bundle_id,appv,language,os,adv_id,gdpr,ccpa,country_code,date
1001,Fatal crash,fatal,MainActivity.java,production,iPhone12,Metal,sess_001,2.1.0,false,flow_001,init,2024-01-01,pub_001,game_001,com.example.app,1.0.0,en,iOS,adv_001,true,false,US,2024-01-01 10:00:00
1002,Network error,error,NetworkManager.java,production,Pixel6,Vulkan,sess_002,2.1.0,false,flow_002,load,2024-01-01,pub_001,game_001,com.example.app,1.0.0,en,Android,adv_002,true,false,US,2024-01-01 10:01:00
1003,Fatal crash,fatal,GameEngine.cpp,production,iPhone12,Metal,sess_003,2.1.0,false,flow_003,play,2024-01-01,pub_001,game_001,com.example.app,1.0.0,en,iOS,adv_003,true,false,UK,2024-01-01 10:02:00
"""
    
    @pytest.fixture
    def temp_csv_file(self, sample_csv_data):
        """Create a temporary CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(sample_csv_data)
            temp_path = f.name
        
        yield temp_path
        
        if os.path.exists(temp_path):
            os.remove(temp_path)
    
    def test_loader_initialization(self):
        """Test LogLoader can be initialized"""
        loader = LogLoader()
        assert loader is not None
        assert hasattr(loader, 'REQUIRED_COLUMNS')
        assert hasattr(loader, 'CATEGORY_COLUMNS')
    
    def test_load_csv_success(self, temp_csv_file):
        """Test successful CSV loading"""
        loader = LogLoader()
        df = loader.load_csv(temp_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert all(col in df.columns for col in loader.REQUIRED_COLUMNS)
    
    def test_load_csv_file_not_found(self):
        """Test error handling for missing file"""
        loader = LogLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_csv("nonexistent_file.csv")
    
    def test_memory_optimization_datetime(self, temp_csv_file):
        """Test that date columns are converted to datetime64"""
        loader = LogLoader()
        df = loader.load_csv(temp_csv_file)
        
        assert pd.api.types.is_datetime64_any_dtype(df['date'])
        assert pd.api.types.is_datetime64_any_dtype(df['sdk_date'])
    
    def test_memory_optimization_category(self, temp_csv_file):
        """Test that category columns are converted to category dtype"""
        loader = LogLoader()
        df = loader.load_csv(temp_csv_file)
        
        for col in ['severity', 'os', 'bundle_id']:
            assert df[col].dtype.name == 'category', f"{col} should be category dtype"
    
    def test_get_memory_usage(self, temp_csv_file):
        """Test memory usage reporting"""
        loader = LogLoader()
        df = loader.load_csv(temp_csv_file)
        
        memory_stats = loader.get_memory_usage(df)
        
        assert 'total_mb' in memory_stats
        assert 'per_column_mb' in memory_stats
        assert 'row_count' in memory_stats
        assert 'column_count' in memory_stats
        assert memory_stats['row_count'] == 3
    
    def test_schema_validation(self, temp_csv_file):
        """Test schema validation works correctly"""
        loader = LogLoader()
        
        df = loader.load_csv(temp_csv_file, validate_schema=True)
        assert df is not None
