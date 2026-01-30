#!/usr/bin/env python3
"""
test_cli.py - Unit tests for find_companies.py CLI interface
"""

import pytest
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open
from src.find_companies import load_config, create_parser


# ============================================================================
# CONFIG LOADING TESTS
# ============================================================================

def test_load_config_missing_file():
    """Test loading config when file doesn't exist"""
    with patch('pathlib.Path.exists', return_value=False):
        config = load_config('nonexistent.yaml')
        assert config == {'defaults': {}, 'profiles': {}}


def test_load_config_valid_file():
    """Test loading valid config file"""
    sample_config = """
defaults:
  output_format: csv
  company_status: Active

profiles:
  test_profile:
    description: "Test profile"
    postcode: "SW1A 1AA"
    radius_miles: 10
    sic_codes: [62020, 62090]
"""
    with patch('builtins.open', mock_open(read_data=sample_config)):
        with patch('pathlib.Path.exists', return_value=True):
            config = load_config('config.yaml')

            assert 'defaults' in config
            assert 'profiles' in config
            assert config['defaults']['output_format'] == 'csv'
            assert 'test_profile' in config['profiles']
            assert config['profiles']['test_profile']['postcode'] == 'SW1A 1AA'


def test_load_config_invalid_yaml():
    """Test loading config with invalid YAML"""
    invalid_yaml = "{ invalid: yaml: content:"

    with patch('builtins.open', mock_open(read_data=invalid_yaml)):
        with patch('pathlib.Path.exists', return_value=True):
            config = load_config('config.yaml')
            # Should return empty structure on error
            assert config == {'defaults': {}, 'profiles': {}}


def test_load_config_empty_file():
    """Test loading empty config file"""
    with patch('builtins.open', mock_open(read_data="")):
        with patch('pathlib.Path.exists', return_value=True):
            config = load_config('config.yaml')
            assert config == {'defaults': {}, 'profiles': {}}


# ============================================================================
# ARGUMENT PARSER TESTS
# ============================================================================

def test_parser_basic_arguments():
    """Test parser accepts basic required arguments"""
    parser = create_parser()
    args = parser.parse_args(['--postcode', 'SW1A 1AA', '--radius', '10'])

    assert args.postcode == 'SW1A 1AA'
    assert args.radius == 10.0


def test_parser_sic_codes():
    """Test parser accepts multiple SIC codes"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--sic', '62020', '62090', '62012'
    ])

    assert args.sic == [62020, 62090, 62012]


def test_parser_account_categories():
    """Test parser accepts multiple account categories"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--categories', 'MICRO ENTITY', 'SMALL'
    ])

    assert args.categories == ['MICRO ENTITY', 'SMALL']


def test_parser_psc_age_filters():
    """Test parser accepts PSC age filters"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--min-psc-age', '50',
        '--max-psc-age', '70'
    ])

    assert args.min_psc_age == 50
    assert args.max_psc_age == 70


def test_parser_company_age_filters():
    """Test parser accepts company age filters"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--min-company-age', '5',
        '--max-company-age', '50'
    ])

    assert args.min_company_age == 5
    assert args.max_company_age == 50


def test_parser_output_format():
    """Test parser accepts output format options"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--format', 'xlsx'
    ])

    assert args.format == 'xlsx'


def test_parser_profile_argument():
    """Test parser accepts profile argument"""
    parser = create_parser()
    args = parser.parse_args(['--profile', 'it_retirement'])

    assert args.profile == 'it_retirement'


def test_parser_config_file():
    """Test parser accepts custom config file"""
    parser = create_parser()
    args = parser.parse_args([
        '--config', 'custom.yaml',
        '--postcode', 'SW1A 1AA',
        '--radius', '10'
    ])

    assert args.config == 'custom.yaml'


def test_parser_list_profiles_flag():
    """Test parser accepts list-profiles flag"""
    parser = create_parser()
    args = parser.parse_args(['--list-profiles'])

    assert args.list_profiles is True


def test_parser_list_categories_flag():
    """Test parser accepts list-categories flag"""
    parser = create_parser()
    args = parser.parse_args(['--list-categories'])

    assert args.list_categories is True


def test_parser_max_results():
    """Test parser accepts max-results argument"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--max-results', '100'
    ])

    assert args.max_results == 100


def test_parser_output_path():
    """Test parser accepts output path"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'SW1A 1AA',
        '--radius', '10',
        '--output', '/tmp/results.csv'
    ])

    assert args.output == Path('/tmp/results.csv')


def test_parser_short_flags():
    """Test parser accepts short flag versions"""
    parser = create_parser()
    args = parser.parse_args([
        '-p', 'SW1A 1AA',
        '-r', '10',
        '-s', '62020',
        '-c', 'MICRO ENTITY',
        '-f', 'xlsx',
        '-o', 'output.xlsx'
    ])

    assert args.postcode == 'SW1A 1AA'
    assert args.radius == 10.0
    assert args.sic == [62020]
    assert args.categories == ['MICRO ENTITY']
    assert args.format == 'xlsx'
    assert args.output == Path('output.xlsx')


def test_parser_defaults():
    """Test parser default values"""
    parser = create_parser()
    args = parser.parse_args(['--postcode', 'SW1A 1AA', '--radius', '10'])

    assert args.format == 'csv'
    assert args.status == 'Active'
    assert args.config == 'config.yaml'
    assert args.list_profiles is False
    assert args.list_categories is False
    assert args.min_psc_age is None
    assert args.max_psc_age is None


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

def test_parser_profile_override():
    """Test profile can be overridden with command-line args"""
    parser = create_parser()
    args = parser.parse_args([
        '--profile', 'it_retirement',
        '--radius', '15'
    ])

    assert args.profile == 'it_retirement'
    assert args.radius == 15.0


def test_parser_complex_query():
    """Test parser with all filter types"""
    parser = create_parser()
    args = parser.parse_args([
        '--postcode', 'EC2R 8AH',
        '--radius', '15',
        '--sic', '43220', '43290',
        '--categories', 'MICRO ENTITY', 'SMALL',
        '--min-company-age', '10',
        '--min-psc-age', '50',
        '--max-psc-age', '70',
        '--format', 'xlsx',
        '--max-results', '500'
    ])

    assert args.postcode == 'EC2R 8AH'
    assert args.radius == 15.0
    assert args.sic == [43220, 43290]
    assert args.categories == ['MICRO ENTITY', 'SMALL']
    assert args.min_company_age == 10
    assert args.min_psc_age == 50
    assert args.max_psc_age == 70
    assert args.format == 'xlsx'
    assert args.max_results == 500
