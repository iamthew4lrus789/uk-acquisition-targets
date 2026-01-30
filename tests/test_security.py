#!/usr/bin/env python3
"""
Security and input validation tests

Tests for SQL injection prevention and input sanitization
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import QueryParams, validate_query_params


# ============================================================================
# SQL Injection Tests
# ============================================================================

def test_sql_injection_in_company_status():
    """Test that SQL injection in company_status is prevented"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        company_status="Active' OR '1'='1"
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "status" in msg.lower() or "invalid" in msg.lower()


def test_malicious_sic_code_string():
    """Test that non-integer SIC codes are rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=["'; DROP TABLE companies; --"]
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "integer" in msg


def test_malicious_sic_code_sql():
    """Test that SQL injection in SIC codes is rejected"""
    # Can't create QueryParams with non-int sic_codes due to type checking,
    # but test validation anyway
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0
    )

    # Manually set invalid sic_codes to test validation
    params.sic_codes = ["62020' OR '1'='1"]

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "integer" in msg.lower()


def test_negative_sic_code():
    """Test that negative SIC codes are rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[-1]
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "range" in msg.lower() or "out of range" in msg.lower()


def test_oversized_sic_code():
    """Test that oversized SIC codes are rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[1000000]  # Way too large
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "range" in msg.lower() or "out of range" in msg.lower()


# ============================================================================
# Type Safety Tests
# ============================================================================

def test_radius_type_validation():
    """Test that non-numeric radius is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0
    )

    # Manually set invalid radius type
    params.radius_miles = "10' OR '1'='1"

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "number" in msg.lower()


def test_valid_integer_radius():
    """Test that integer radius is accepted"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10  # Integer, not float
    )

    valid, msg = validate_query_params(params)
    assert valid == True


# ============================================================================
# Whitelist Validation Tests
# ============================================================================

def test_valid_company_statuses():
    """Test that all valid company statuses are accepted"""
    valid_statuses = ['Active', 'Dissolved', 'Liquidation', 'Administration']

    for status in valid_statuses:
        params = QueryParams(
            postcode="SW1A 1AA",
            radius_miles=10.0,
            company_status=status
        )

        valid, msg = validate_query_params(params)
        assert valid == True, f"Status '{status}' should be valid but got: {msg}"


def test_invalid_company_status():
    """Test that invalid company status is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        company_status="InvalidStatus"
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "status" in msg.lower()


def test_case_sensitive_company_status():
    """Test that company status is case-sensitive"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        company_status="active"  # lowercase
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "status" in msg.lower()


# ============================================================================
# Bounds Checking Tests
# ============================================================================

def test_radius_zero():
    """Test that zero radius is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=0
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "greater than 0" in msg


def test_radius_negative():
    """Test that negative radius is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=-10
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "greater than 0" in msg


def test_radius_too_large():
    """Test that radius > 500 is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=501
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "500" in msg


def test_psc_age_below_minimum():
    """Test that PSC age < 16 is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=15
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "16" in msg


def test_psc_age_above_maximum():
    """Test that PSC age > 120 is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        max_psc_age=121
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "120" in msg


# ============================================================================
# Account Category Validation Tests
# ============================================================================

def test_invalid_account_category():
    """Test that invalid account category is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        account_categories=['INVALID_CATEGORY']
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "category" in msg.lower()


def test_sql_injection_in_account_category():
    """Test that SQL injection in account category is rejected"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        account_categories=["MICRO ENTITY' OR '1'='1"]
    )

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "category" in msg.lower()


# ============================================================================
# Required Field Tests
# ============================================================================

def test_none_postcode_rejected():
    """Test that None postcode is rejected by validation"""
    # Manually create params with None to test validation
    from dataclasses import replace
    params = QueryParams.__new__(QueryParams)
    params.postcode = None
    params.radius_miles = 10.0
    params.company_status = 'Active'
    params.account_categories = None
    params.sic_codes = None
    params.max_company_age_years = None
    params.min_company_age_years = None
    params.min_psc_age = None
    params.max_psc_age = None
    params.output_format = 'csv'
    params.output_path = None
    params.max_results = None

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "required" in msg.lower()


def test_none_radius_rejected():
    """Test that None radius is rejected by validation"""
    # Manually create params with None to test validation
    params = QueryParams.__new__(QueryParams)
    params.postcode = "SW1A 1AA"
    params.radius_miles = None
    params.company_status = 'Active'
    params.account_categories = None
    params.sic_codes = None
    params.max_company_age_years = None
    params.min_company_age_years = None
    params.min_psc_age = None
    params.max_psc_age = None
    params.output_format = 'csv'
    params.output_path = None
    params.max_results = None

    valid, msg = validate_query_params(params)
    assert valid == False
    assert "required" in msg.lower()
