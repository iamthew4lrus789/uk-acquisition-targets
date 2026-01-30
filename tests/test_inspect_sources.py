#!/usr/bin/env python3
"""
Test data inspection tool (inspect_sources.py)

This module tests the data validation and inspection functionality
that verifies raw data structure before processing.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.inspect_sources import main as check_files


def test_check_files_no_files():
    """Test file checking with no files found"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_glob.return_value = []
        
        with pytest.raises(SystemExit):
            check_files()


def test_check_files_missing_required():
    """Test file checking with missing required files"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        # Only return some files, missing others
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        
        # Mock glob to return files for some patterns only
        def glob_side_effect(pattern):
            if 'BasicCompanyData' in pattern:
                return [mock_file]
            elif 'persons-with-significant-control' in pattern:
                return []  # Missing PSC file
            elif 'ONSPD' in pattern:
                return [mock_file]
            return []
        
        mock_glob.side_effect = glob_side_effect
        
        with pytest.raises(SystemExit):
            check_files()


def test_check_files_all_present():
    """Test file checking with all required files present"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        
        # Mock glob to return files for all patterns
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print'):
            # Should not raise
            check_files()


def test_check_files_small_file():
    """Test file checking with suspiciously small file"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        small_file = MagicMock()
        small_file.exists.return_value = True
        small_file.stat.return_value.st_size = 100  # Too small
        small_file.name = 'BasicCompanyDataAsOneFile-2025-01-01.csv'
        
        mock_glob.return_value = [small_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should warn about small file
            assert any('small' in str(call).lower() for call in mock_print.call_args_list)


def test_check_files_corrupt_file():
    """Test file checking with corrupt/unreadable file"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        mock_file.name = 'corrupt.csv'
        
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should still process without crashing
            assert any('Checking' in str(call) for call in mock_print.call_args_list)


def test_check_files_companies_structure():
    """Test companies file structure validation"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        mock_file.name = 'BasicCompanyDataAsOneFile-2025-01-01.csv'
        
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should show companies file info
            assert any('Companies' in str(call) for call in mock_print.call_args_list)


def test_check_files_psc_structure():
    """Test PSC file structure validation"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 5000000
        mock_file.name = 'persons-with-significant-control-snapshot-2025-12-30.txt'
        
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should show PSC file info
            assert any('PSC' in str(call) for call in mock_print.call_args_list)


def test_check_files_postcodes_structure():
    """Test postcodes file structure validation"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 200000
        mock_file.name = 'ONSPD_FEB_2025/Data/ONSPD_FEB_2025_UK.csv'
        
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should show postcode file info
            assert any('Postcode' in str(call) for call in mock_print.call_args_list)


def test_check_files_summary_output():
    """Test that summary output is generated"""
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        
        mock_glob.return_value = [mock_file]
        
        with patch('builtins.print') as mock_print:
            check_files()
            
            # Should generate summary
            calls = [str(call) for call in mock_print.call_args_list]
            assert any('Summary' in call for call in calls)
            assert any('Total' in call for call in calls)


def test_check_files_performance():
    """Test that file checking completes in reasonable time"""
    import time
    
    with patch('src.inspect_sources.Path.glob') as mock_glob:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000
        
        mock_glob.return_value = [mock_file]
        
        start = time.time()
        check_files()
        duration = time.time() - start
        
        # Should complete quickly
        assert duration < 1.0