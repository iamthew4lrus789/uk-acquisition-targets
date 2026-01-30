"""
Test PSC tenure filtering functionality
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import validate_query_params, build_query, QueryParams


def test_tenure_validation_valid():
    """Test valid tenure parameters"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_tenure_years=1,
        max_psc_tenure_years=10
    )
    valid, msg = validate_query_params(params)
    assert valid == True
    assert msg == "Valid"


def test_tenure_validation_invalid_min():
    """Test invalid minimum tenure"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_tenure_years=0
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "≥ 1" in msg


def test_tenure_validation_invalid_max():
    """Test invalid maximum tenure"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        max_psc_tenure_years=101
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "≤ 100" in msg


def test_tenure_validation_inverted_range():
    """Test inverted tenure range"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_tenure_years=10,
        max_psc_tenure_years=1
    )
    valid, msg = validate_query_params(params)
    assert valid == False
    assert "cannot exceed maximum" in msg


def test_tenure_query_generation():
    """Test that tenure parameters generate correct SQL"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_tenure_years=2,
        max_psc_tenure_years=5
    )
    query = build_query(51.5, -0.1, params)
    
    # Should contain tenure filtering logic based on notified_on date
    assert "notified_on" in query
    assert "YEAR(CURRENT_DATE)" in query
    assert "YEAR(p.notified_on)" in query


def test_tenure_query_min_only():
    """Test query with only minimum tenure"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_tenure_years=3
    )
    query = build_query(51.5, -0.1, params)
    
    assert "notified_on" in query
    assert ">= 3" in query or ">=3" in query


def test_tenure_query_max_only():
    """Test query with only maximum tenure"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        max_psc_tenure_years=7
    )
    query = build_query(51.5, -0.1, params)
    
    assert "notified_on" in query
    assert "<= 7" in query or "<=7" in query


def test_tenure_with_psc_age_combination():
    """Test tenure combined with PSC age filtering"""
    params = QueryParams(
        postcode="SW1A 1AA", 
        radius_miles=10.0,
        min_psc_age=50,
        max_psc_age=70,
        min_psc_tenure_years=2,
        max_psc_tenure_years=10
    )
    query = build_query(51.5, -0.1, params)
    
    # Should contain both age and tenure filters
    assert "approximate_age >= 50" in query
    assert "approximate_age <= 70" in query
    assert "notified_on" in query
