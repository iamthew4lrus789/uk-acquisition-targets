"""
Test query parameter validation
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import validate_query_params, QueryParams


def test_valid_params():
    """Test valid parameter set"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=10.0)
    valid, msg = validate_query_params(params)
    assert valid == True
    assert msg == "Valid"


def test_invalid_postcode_too_short():
    """Test postcode that's too short"""
    params = QueryParams(postcode="ABC", radius_miles=10.0)
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "Invalid postcode format" in msg


def test_invalid_postcode_too_long():
    """Test postcode that's too long"""
    params = QueryParams(postcode="ABCDEFGHIJ", radius_miles=10.0)
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "Invalid postcode format" in msg


def test_invalid_radius_zero():
    """Test radius of zero"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=0)
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "greater than 0" in msg


def test_invalid_radius_negative():
    """Test negative radius"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=-5)
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "greater than 0" in msg


def test_invalid_radius_too_large():
    """Test radius exceeding maximum"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=501)
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "500 miles" in msg


def test_invalid_sic_code_not_integer():
    """Test non-integer SIC code"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=["62012"]  # String instead of int
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "must be integer" in msg


def test_invalid_sic_code_too_short():
    """Test SIC code with fewer than 5 digits"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[123]
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "5 digits" in msg


def test_invalid_sic_code_too_long():
    """Test SIC code with more than 5 digits"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[123456]
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "range" in msg.lower() or "5 digits" in msg


def test_valid_sic_codes():
    """Test valid SIC codes"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[62012, 62020, 43220]
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_empty_sic_codes_list():
    """Test empty SIC codes list"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[]
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "cannot be empty" in msg


def test_invalid_account_category():
    """Test invalid account category"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        account_categories=['INVALID_CATEGORY']
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "Invalid account category" in msg


def test_valid_account_categories():
    """Test valid account categories"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        account_categories=['MICRO ENTITY', 'SMALL']
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_min_psc_age_too_young():
    """Test minimum PSC age below threshold"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=15
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "≥ 16" in msg


def test_max_psc_age_too_old():
    """Test maximum PSC age above threshold"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        max_psc_age=121
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "≤ 120" in msg


def test_age_range_inverted():
    """Test minimum age greater than maximum age"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=70,
        max_psc_age=50
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "cannot exceed maximum" in msg


def test_valid_age_range():
    """Test valid age range"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=50,
        max_psc_age=70
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_invalid_output_format():
    """Test invalid output format"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        output_format='pdf'
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "csv" in msg or "xlsx" in msg


def test_valid_csv_format():
    """Test valid CSV output format"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        output_format='csv'
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_valid_xlsx_format():
    """Test valid Excel output format"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        output_format='xlsx'
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_complex_valid_params():
    """Test complex but valid parameter set"""
    params = QueryParams(
        postcode="EC2R 8AH",
        radius_miles=25.5,
        sic_codes=[62012, 62020],
        account_categories=['MICRO ENTITY', 'SMALL', 'MEDIUM'],
        min_psc_age=45,
        max_psc_age=75,
        output_format='xlsx'
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_company_age_below_minimum():
    """Test minimum company age cannot be negative"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_company_age_years=-1
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "≥ 0" in msg or ">= 0" in msg.lower()


def test_company_age_above_maximum():
    """Test maximum company age has reasonable upper bound"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        max_company_age_years=201
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "200" in msg


def test_company_age_range_inverted():
    """Test minimum company age cannot exceed maximum"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_company_age_years=50,
        max_company_age_years=10
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "cannot exceed" in msg.lower()


def test_valid_company_age_range():
    """Test valid company age range"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_company_age_years=5,
        max_company_age_years=50
    )
    valid, msg = validate_query_params(params)
    assert valid == True


def test_company_status_validation():
    """Test company status must be from whitelist"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        company_status="InvalidStatus"
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "status" in msg.lower()
