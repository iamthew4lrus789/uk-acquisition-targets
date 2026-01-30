#!/usr/bin/env python3
"""
Test CLI companion text file feature
Tests for creating .txt files with CLI commands alongside CSV/XLSX outputs
"""

import pytest
import sys
import os
import tempfile
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.find_companies import create_command_file


def test_create_command_file_basic():
    """Test basic command file creation with minimal parameters"""
    # Create a mock args object
    class MockArgs:
        def __init__(self):
            self.config = 'config.yaml'
            self.profile = None
            self.postcode = 'SW1A1AA'
            self.radius = 2.0
            self.sic = None
            self.categories = None
            self.status = 'Active'
            self.min_psc_age = None
            self.max_psc_age = None
            self.min_psc_tenure = 5
            self.max_psc_tenure = 15
            self.strict_psc_tenure = False
            self.min_company_age = None
            self.max_company_age = None
            self.format = 'csv'
            self.output = Path('test_output.csv')
            self.max_results = None

    # Mock sys.argv
    original_argv = sys.argv
    try:
        sys.argv = ['src/find_companies.py', '--postcode', 'SW1A1AA', '--radius', '2', 
                   '--min-psc-tenure', '5', '--max-psc-tenure', '15', 
                   '--format', 'csv', '--output', 'test_output.csv']
        
        args = MockArgs()
        output_path = Path('test_output.csv')
        
        # Call the function
        result = create_command_file(args, output_path)
        
        # Verify the result
        assert result.exists(), "Command file should be created"
        assert result.suffix == '.txt', "Should have .txt extension"
        assert result.name == 'test_output.txt', "Should have same base name as output"
        
        # Check content
        content = result.read_text()
        assert '# Companies House Query Command' in content
        assert '# Generated:' in content
        assert '# Results file: test_output.csv' in content
        assert '# Command:' in content
        assert 'find_companies.py' in content
        assert '--postcode' in content
        assert 'SW1A1AA' in content
        assert '--radius' in content
        assert '2' in content
        assert '--min-psc-tenure' in content
        assert '5' in content
        assert '--max-psc-tenure' in content
        assert '15' in content
        # Note: --format csv is not included because csv is the default
        assert '--output' in content
        assert 'test_output.csv' in content
        
    finally:
        # Restore original argv
        sys.argv = original_argv
        # Clean up
        if result.exists():
            result.unlink()


def test_create_command_file_with_profile():
    """Test command file creation when using a profile"""
    class MockArgs:
        def __init__(self):
            self.config = 'config.yaml'
            self.profile = 'it_retirement'
            self.postcode = None
            self.radius = None
            self.sic = None
            self.categories = None
            self.status = 'Active'
            self.min_psc_age = None
            self.max_psc_age = None
            self.min_psc_tenure = None
            self.max_psc_tenure = None
            self.strict_psc_tenure = False
            self.min_company_age = None
            self.max_company_age = None
            self.format = 'csv'
            self.output = Path('profile_output.csv')
            self.max_results = None

    original_argv = sys.argv
    try:
        sys.argv = ['src/find_companies.py', '--profile', 'it_retirement', 
                   '--output', 'profile_output.csv']
        
        args = MockArgs()
        output_path = Path('profile_output.csv')
        
        result = create_command_file(args, output_path)
        
        assert result.exists()
        content = result.read_text()
        assert '--profile' in content
        assert 'it_retirement' in content
        
    finally:
        sys.argv = original_argv
        if result.exists():
            result.unlink()


def test_create_command_file_all_parameters():
    """Test command file creation with all possible parameters"""
    class MockArgs:
        def __init__(self):
            self.config = 'custom_config.yaml'
            self.profile = None
            self.postcode = 'SW1A1AA'
            self.radius = 10.5
            self.sic = [62020, 62090]
            self.categories = ['MICRO ENTITY', 'SMALL']
            self.status = 'Dissolved'
            self.min_psc_age = 60
            self.max_psc_age = 75
            self.min_psc_tenure = 2
            self.max_psc_tenure = 10
            self.strict_psc_tenure = True
            self.min_company_age = 5
            self.max_company_age = 20
            self.format = 'xlsx'
            self.output = Path('full_output.xlsx')
            self.max_results = 1000

    original_argv = sys.argv
    try:
        sys.argv = [
            'src/find_companies.py',
            '--config', 'custom_config.yaml',
            '--postcode', 'SW1A1AA',
            '--radius', '10.5',
            '--sic', '62020', '62090',
            '--categories', 'MICRO ENTITY', 'SMALL',
            '--status', 'Dissolved',
            '--min-psc-age', '60',
            '--max-psc-age', '75',
            '--min-psc-tenure', '2',
            '--max-psc-tenure', '10',
            '--strict-psc-tenure',
            '--min-company-age', '5',
            '--max-company-age', '20',
            '--format', 'xlsx',
            '--output', 'full_output.xlsx',
            '--max-results', '1000'
        ]
        
        args = MockArgs()
        output_path = Path('full_output.xlsx')
        
        result = create_command_file(args, output_path)
        
        assert result.exists()
        content = result.read_text()
        
        # Check all parameters are included
        assert '--config' in content
        assert 'custom_config.yaml' in content
        assert '--postcode' in content
        assert 'SW1A1AA' in content
        assert '--radius' in content
        assert '10.5' in content
        assert '--sic' in content
        assert '62020' in content and '62090' in content
        assert '--categories' in content
        assert 'MICRO ENTITY' in content and 'SMALL' in content
        assert '--status' in content
        assert 'Dissolved' in content
        assert '--min-psc-age' in content
        assert '60' in content
        assert '--max-psc-age' in content
        assert '75' in content
        assert '--min-psc-tenure' in content
        assert '2' in content
        assert '--max-psc-tenure' in content
        assert '10' in content
        assert '--strict-psc-tenure' in content
        assert '--min-company-age' in content
        assert '5' in content
        assert '--max-company-age' in content
        assert '20' in content
        assert '--format' in content
        assert 'xlsx' in content
        assert '--output' in content
        assert 'full_output.xlsx' in content
        assert '--max-results' in content
        assert '1000' in content
        
    finally:
        sys.argv = original_argv
        if result.exists():
            result.unlink()


def test_create_command_file_auto_generated_name():
    """Test command file creation with auto-generated output name"""
    class MockArgs:
        def __init__(self):
            self.config = 'config.yaml'
            self.profile = None
            self.postcode = 'SW1A1AA'
            self.radius = 2.0
            self.sic = None
            self.categories = None
            self.status = 'Active'
            self.min_psc_age = None
            self.max_psc_age = None
            self.min_psc_tenure = None
            self.max_psc_tenure = None
            self.strict_psc_tenure = False
            self.min_company_age = None
            self.max_company_age = None
            self.format = 'csv'
            self.output = None
            self.max_results = None

    original_argv = sys.argv
    try:
        sys.argv = ['src/find_companies.py', '--postcode', 'SW1A1AA', '--radius', '2']
        
        args = MockArgs()
        # Simulate auto-generated output path
        output_path = Path(f"companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        result = create_command_file(args, output_path)
        
        assert result.exists()
        assert result.suffix == '.txt'
        assert result.stem == output_path.stem
        
        content = result.read_text()
        assert '--postcode' in content
        assert 'SW1A1AA' in content
        assert '--radius' in content
        assert '2' in content
        
    finally:
        sys.argv = original_argv
        if result.exists():
            result.unlink()


if __name__ == "main":
    # Run tests to confirm they fail (expected before implementation)
    pytest.main([__file__, "-v"])
