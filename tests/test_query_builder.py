"""
Test SQL query building logic
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import build_query, QueryParams


def test_basic_query():
    """Test basic query with only postcode and radius"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=10.0)
    query = build_query(51.5, -0.1, params)

    assert "companies_with_coords" in query
    assert "sic_aggregates" in query  # New aggregation CTE
    assert "psc_aggregates" in query  # New aggregation CTE
    assert "companies_enriched" in query  # New enriched CTE
    assert "3959" in query  # Earth radius in miles (Haversine)
    assert "10.0" in query  # radius
    assert "distance_miles" in query
    assert "ORDER BY distance_miles" in query


def test_query_with_sic_codes():
    """Test query with SIC code filter"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        sic_codes=[62012, 62020]
    )
    query = build_query(51.5, -0.1, params)

    assert "company_sic.parquet" in query
    assert "62012" in query
    assert "62020" in query
    assert "companies_with_sic" in query
    # Enhanced fields should still be present
    assert "sic_aggregates" in query
    assert "psc_aggregates" in query


def test_query_with_psc_age():
    """Test query with PSC age filter"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=50,
        max_psc_age=70
    )
    query = build_query(51.5, -0.1, params)

    assert "psc.parquet" in query
    assert "approximate_age >= 50" in query
    assert "approximate_age <= 70" in query
    assert "companies_with_psc_age" in query
    # Enhanced fields should still be present
    assert "sic_aggregates" in query
    assert "psc_aggregates" in query
    
    # Test that CTE is properly separated with comma
    # The CTE should start with a comma to separate from previous CTE
    cte_start = query.find("companies_with_psc_age AS (")
    if cte_start > 0:
        # Check that there's a comma before the CTE definition
        context_before = query[cte_start-10:cte_start]
        assert "," in context_before, f"CTE should be preceded by comma. Context: {context_before}"


def test_query_with_account_categories():
    """Test query with account category filter"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        account_categories=['MICRO ENTITY', 'SMALL']
    )
    query = build_query(51.5, -0.1, params)

    assert "MICRO ENTITY" in query
    assert "SMALL" in query
    assert "AccountCategory IN" in query
    # Enhanced fields should still be present
    assert "sic_aggregates" in query
    assert "companies_enriched" in query


def test_query_with_all_filters():
    """Test query with all filters combined"""
    params = QueryParams(
        postcode="EC2R 8AH",
        radius_miles=15.0,
        sic_codes=[43220, 43290],
        min_psc_age=50,
        max_psc_age=70,
        account_categories=['MICRO ENTITY', 'SMALL']
    )
    query = build_query(51.4543, -0.9781, params)

    # Check all filters are present
    assert "companies_with_coords" in query
    assert "companies_in_radius" in query
    assert "companies_with_sic" in query
    assert "companies_with_psc_age" in query
    assert "15.0" in query  # radius
    assert "43220" in query  # SIC code
    assert "43290" in query  # SIC code
    assert "approximate_age >= 50" in query
    assert "approximate_age <= 70" in query
    assert "MICRO ENTITY" in query
    # Enhanced aggregations should be present
    assert "sic_aggregates" in query
    assert "psc_aggregates" in query
    assert "companies_enriched" in query


def test_query_coordinates_embedded():
    """Test that coordinates are properly embedded in Haversine formula"""
    target_lat = 51.501009
    target_lon = -0.141588
    params = QueryParams(postcode="SW1A 1AA", radius_miles=5.0)
    query = build_query(target_lat, target_lon, params)

    assert str(target_lat) in query
    assert str(target_lon) in query
    # Enhanced CTEs should be present
    assert "sic_aggregates" in query


def test_query_company_status():
    """Test that company status filter is applied"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        company_status='Active'
    )
    query = build_query(51.5, -0.1, params)

    assert "CompanyStatus = 'Active'" in query
    # Enhanced CTEs should be present
    assert "companies_enriched" in query


def test_query_min_psc_age_only():
    """Test query with only minimum PSC age"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_age=50
    )
    query = build_query(51.5, -0.1, params)

    assert "approximate_age >= 50" in query
    assert "approximate_age <=" not in query
    # Enhanced CTEs should be present
    assert "psc_aggregates" in query


def test_query_max_psc_age_only():
    """Test query with only maximum PSC age"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        max_psc_age=70
    )
    query = build_query(51.5, -0.1, params)

    assert "approximate_age <= 70" in query
    assert "approximate_age >=" not in query
    # Enhanced CTEs should be present
    assert "psc_aggregates" in query


def test_query_output_columns():
    """Test that enhanced output columns are correct"""
    params = QueryParams(postcode="SW1A 1AA", radius_miles=10.0)
    query = build_query(51.5, -0.1, params)

    # Check expected output columns (enhanced set)
    expected_columns = [
        "CompanyNumber", "CompanyName", "Postcode", "DistanceMiles",
        "CompanyStatus", "CompanyCategory", "AccountCategory",
        "IncorporationDate", "CompanyAgeYears", "LastAccountsDate",
        "SicCodeCount", "PrimarySicCode", "PrimarySicDescription",
        "PscCount", "YoungestPscAge", "OldestPscAge", "PscLastUpdated"
    ]
    for col in expected_columns:
        assert col in query
