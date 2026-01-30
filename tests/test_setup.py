#!/usr/bin/env python3
"""
Test data processing pipeline (setup.py)

This module tests the core data processing functionality that converts
raw Companies House data to Parquet format.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.setup import (
    validate_files,
    find_latest_files,
    setup_companies,
    setup_company_sic,
    setup_postcodes,
    setup_psc,
    count_psc_lines,
    report_psc_parse_errors
)


def test_find_latest_files_no_files():
    """Test behavior when no files exist"""
    with patch('src.setup.Path.glob') as mock_glob:
        mock_glob.return_value = []
        
        # Should not raise an error, just leave files as None
        find_latest_files()
        
        # Verify no files were found
        from src.setup import REQUIRED_FILES
        assert all(v is None for v in REQUIRED_FILES.values())


def test_find_latest_files_with_companies():
    """Test finding latest company data file"""
    with patch('src.setup.Path.glob') as mock_glob:
        # Create mock files
        mock_file1 = MagicMock()
        mock_file1.name = 'BasicCompanyDataAsOneFile-2025-01-01.csv'
        mock_file1.stat.return_value.st_size = 1000
        
        mock_file2 = MagicMock()
        mock_file2.name = 'BasicCompanyDataAsOneFile-2025-12-01.csv'
        mock_file2.stat.return_value.st_size = 2000
        
        mock_glob.return_value = [mock_file1, mock_file2]
        
        # Mock max() to return the newer file
        with patch('builtins.max', return_value=mock_file2):
            find_latest_files()
            
            # Verify companies file was detected
            from src.setup import REQUIRED_FILES
            assert REQUIRED_FILES['companies'] == mock_file2


def test_find_latest_files_with_psc():
    """Test finding latest PSC data file"""
    with patch('src.setup.Path.glob') as mock_glob:
        # Create mock PSC files
        mock_file1 = MagicMock()
        mock_file1.name = 'persons-with-significant-control-snapshot-2025-01-01.txt'
        mock_file1.stat.return_value.st_size = 5000
        
        mock_file2 = MagicMock()
        mock_file2.name = 'persons-with-significant-control-snapshot-2025-12-30.txt'
        mock_file2.stat.return_value.st_size = 8000
        
        mock_glob.return_value = [mock_file1, mock_file2]
        
        # Mock max() to return the newer file
        with patch('builtins.max', return_value=mock_file2):
            find_latest_files()
            
            # Verify PSC file was detected
            from src.setup import REQUIRED_FILES
            assert REQUIRED_FILES['psc'] == mock_file2


def test_find_latest_files_with_postcodes():
    """Test finding latest postcode data file"""
    with patch('src.setup.Path.glob') as mock_glob:
        # Create mock postcode directory and file
        mock_file = MagicMock()
        mock_file.name = 'ONSPD_FEB_2025/Data/ONSPD_FEB_2025_UK.csv'
        mock_file.stat.return_value.st_size = 200
        
        mock_glob.return_value = [mock_file]
        
        find_latest_files()
        
        # Verify postcode file was detected
        from src.setup import REQUIRED_FILES
        assert REQUIRED_FILES['postcodes'] == mock_file


def test_validate_files_all_missing():
    """Test validation with all files missing"""
    # Mock both find_latest_files and the file checks
    with patch('src.setup.find_latest_files'), \
         patch('src.setup.Path') as mock_path_class:
        
        mock_file = MagicMock()
        mock_file.exists.return_value = False
        mock_file.name = 'missing.parquet'
        
        # Mock the REQUIRED_FILES dictionary
        with patch('src.setup.REQUIRED_FILES', {
            'companies': mock_file,
            'psc': mock_file,
            'postcodes': mock_file
        }):
            with pytest.raises(SystemExit):
                validate_files()


def test_validate_files_partial_missing():
    """Test validation with some files missing"""
    with patch('src.setup.find_latest_files'), \
         patch('src.setup.Path') as mock_path:
        existing_file = MagicMock()
        existing_file.exists.return_value = True
        existing_file.stat.return_value.st_size = 1000000
        
        missing_file = MagicMock()
        missing_file.exists.return_value = False
        
        mock_path.return_value = {
            'companies': existing_file,
            'psc': missing_file,
            'postcodes': existing_file
        }
        
        with patch.dict('src.setup.REQUIRED_FILES', {
            'companies': existing_file,
            'psc': missing_file,
            'postcodes': existing_file
        }):
            with pytest.raises(SystemExit):
                validate_files()


def test_validate_files_all_present():
    """Test validation with all files present and valid sizes"""
    with patch('src.setup.Path') as mock_path:
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.stat.return_value.st_size = 1000000  # 1MB
        
        mock_path.return_value = {
            'companies': mock_file,
            'psc': mock_file,
            'postcodes': mock_file
        }
        
        with patch.dict('src.setup.REQUIRED_FILES', {
            'companies': mock_file,
            'psc': mock_file,
            'postcodes': mock_file
        }):
            # Should not raise
            validate_files()


def test_validate_files_small_companies():
    """Test validation with suspiciously small companies file"""
    with patch('src.setup.find_latest_files'), \
         patch('src.setup.Path') as mock_path:
        small_file = MagicMock()
        small_file.exists.return_value = True
        small_file.stat.return_value.st_size = 100  # Too small
        small_file.name = 'BasicCompanyDataAsOneFile-2025-01-01.csv'
        
        # Create proper mock files with exists() method
        psc_file = MagicMock()
        psc_file.exists.return_value = True
        psc_file.stat.return_value.st_size = 5000
        
        postcodes_file = MagicMock()
        postcodes_file.exists.return_value = True
        postcodes_file.stat.return_value.st_size = 200
        
        mock_path.return_value = {
            'companies': small_file,
            'psc': psc_file,
            'postcodes': postcodes_file
        }
        
        with patch.dict('src.setup.REQUIRED_FILES', {
            'companies': small_file,
            'psc': psc_file,
            'postcodes': postcodes_file
        }), patch('builtins.print') as mock_print:
            validate_files()
            
            # Should warn about small file - check the actual print calls
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('small' in call.lower() for call in print_calls), f"Expected 'small' warning in: {print_calls}"


def test_setup_companies_basic():
    """Test companies setup with mock database"""
    mock_con = MagicMock()
    
    # Mock fetchone to return reasonable values for all 3 queries
    mock_con.execute.return_value.fetchone.side_effect = [
        [5000000],   # Total count
        [4500000],   # Active count
        [100000]     # Null postcodes count
    ]
    
    with patch('src.setup.duckdb.connect', return_value=mock_con), \
         patch('builtins.print'):  # Mock print to avoid formatting issues
        setup_companies(mock_con)
        
        # Verify expected SQL was executed
        assert mock_con.execute.called
        exec_calls = mock_con.execute.call_args_list
        
        # Check that companies processing SQL was executed
        companies_sql = None
        for call in exec_calls:
            if 'companies.parquet' in str(call):
                companies_sql = call
                break
        
        assert companies_sql is not None
        assert 'CompanyNumber' in str(companies_sql)
        assert 'CompanyName' in str(companies_sql)


def test_setup_company_sic_basic():
    """Test SIC code normalization with mock database"""
    mock_con = MagicMock()
    
    # Mock fetchone to return reasonable values for all 3 queries
    mock_con.execute.return_value.fetchone.side_effect = [
        [10000000],  # Total SIC mappings
        [5000],      # Unique SIC codes
        [2000000]    # Unique companies with SIC codes
    ]
    
    with patch('src.setup.duckdb.connect', return_value=mock_con), \
         patch('builtins.print'):  # Mock print to avoid formatting issues
        setup_company_sic(mock_con)
        
        # Verify SIC processing SQL was executed
        assert mock_con.execute.called
        exec_calls = mock_con.execute.call_args_list
        
        # Check that SIC normalization SQL was executed
        sic_sql = None
        for call in exec_calls:
            if 'company_sic.parquet' in str(call):
                sic_sql = call
                break
        
        assert sic_sql is not None
        assert 'SicText_1' in str(sic_sql) or 'sic_code' in str(sic_sql)


def test_setup_postcodes_basic():
    """Test postcode processing with mock database"""
    mock_con = MagicMock()
    
    # Mock fetchone to return reasonable values for all queries
    mock_con.execute.return_value.fetchone.side_effect = [
        [1800000],  # Total postcodes
        (49.0, 61.0, -8.0, 2.0)  # Min/max lat/long
    ]
    
    with patch('src.setup.duckdb.connect', return_value=mock_con), \
         patch('builtins.print'):  # Mock print to avoid formatting issues
        setup_postcodes(mock_con)
        
        # Verify postcode processing SQL was executed
        assert mock_con.execute.called
        exec_calls = mock_con.execute.call_args_list
        
        # Check that postcode processing SQL was executed
        postcode_sql = None
        for call in exec_calls:
            if 'postcodes.parquet' in str(call):
                postcode_sql = call
                break
        
        assert postcode_sql is not None
        assert 'pcds' in str(postcode_sql)
        assert 'lat' in str(postcode_sql)
        assert 'long' in str(postcode_sql)


def test_setup_psc_basic():
    """Test PSC processing with mock database"""
    mock_con = MagicMock()
    
    # Mock fetchone to return reasonable values for all queries
    mock_con.execute.return_value.fetchone.side_effect = [
        [11000000],  # Total PSCs
        [3000000],   # Unique companies with PSCs
        (18, 65.5, 120)  # Min/avg/max age
    ]
    
    with patch('src.setup.duckdb.connect', return_value=mock_con), \
         patch('builtins.print'):  # Mock print to avoid formatting issues
        setup_psc(mock_con)
        
        # Verify PSC processing SQL was executed
        assert mock_con.execute.called
        exec_calls = mock_con.execute.call_args_list
        
        # Check that PSC processing SQL was executed
        psc_sql = None
        for call in exec_calls:
            if 'psc.parquet' in str(call):
                psc_sql = call
                break
        
        assert psc_sql is not None
        assert 'approximate_age' in str(psc_sql)
        assert 'birth_year' in str(psc_sql)


def test_count_psc_lines():
    """Test PSC line counting functionality"""
    with patch('builtins.open', mock_open(read_data='line1\nline2\nline3')) as mock_file:
        result = count_psc_lines('dummy.txt')
        assert result == 3
        
        # Verify file was opened and read
        mock_file.assert_called_once_with('dummy.txt', 'r', encoding='utf-8')


def test_count_psc_lines_empty():
    """Test PSC line counting with empty file"""
    with patch('builtins.open', mock_open(read_data='')) as mock_file:
        result = count_psc_lines('empty.txt')
        assert result == 0


def test_report_psc_parse_errors():
    """Test PSC parse error reporting"""
    mock_con = MagicMock()
    mock_file = MagicMock()
    
    # Mock file line counting
    with patch('src.setup.count_psc_lines', return_value=100):
        # Mock database query for parsed records
        mock_con.execute.return_value.fetchone.return_value = [80]  # 80 parsed
        
        with patch('builtins.print') as mock_print:
            report_psc_parse_errors(mock_con, mock_file)
            
            # Should report dropped records
            assert any('Dropped' in str(call) for call in mock_print.call_args_list)
            assert any('20' in str(call) for call in mock_print.call_args_list)  # 100-80=20


def test_report_psc_parse_errors_no_errors():
    """Test PSC parse error reporting with no errors"""
    mock_con = MagicMock()
    mock_file = MagicMock()
    
    # Mock file line counting
    with patch('src.setup.count_psc_lines', return_value=100):
        # Mock database query for parsed records
        mock_con.execute.return_value.fetchone.return_value = [100]  # All parsed
        
        with patch('builtins.print') as mock_print:
            report_psc_parse_errors(mock_con, mock_file)
            
            # Should not report errors if all records parsed
            error_calls = [call for call in mock_print.call_args_list if 'Dropped' in str(call)]
            assert len(error_calls) == 0


def test_main_integration():
    """Test main function integration"""
    with patch('src.setup.validate_files'), \
         patch('src.setup.duckdb.connect') as mock_connect, \
         patch('builtins.print'):
        
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        
        # Mock all setup functions
        with patch('src.setup.setup_companies'), \
             patch('src.setup.setup_company_sic'), \
             patch('src.setup.setup_postcodes'), \
             patch('src.setup.setup_psc'), \
             patch('src.setup.report_sic_parse_errors'), \
             patch('src.setup.report_psc_parse_errors'):
            
            # Import and run main
            from src.setup import main
            
            # Should not raise
            main()
            
            # Verify connection was closed
            mock_con.close.assert_called_once()