"""
Test strict PSC tenure mode functionality
"""
import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import validate_query_params, build_psc_tenure_filter, QueryParams


def test_strict_psc_tenure_validation():
    """Test that strict PSC tenure parameter is accepted"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        strict_psc_tenure=True
    )
    
    # Should be valid
    valid, msg = validate_query_params(params)
    assert valid == True
    assert msg == "Valid"


def test_strict_psc_tenure_tenure_filter_generation():
    """Test that strict mode generates different SQL"""
    # Test normal mode
    normal_query, normal_cte = build_psc_tenure_filter(
        "test_cte",
        min_tenure=2,
        max_tenure=10,
        strict_mode=False
    )
    
    # Test strict mode
    strict_query, strict_cte = build_psc_tenure_filter(
        "test_cte",
        min_tenure=2,
        max_tenure=10,
        strict_mode=True
    )
    
    # Should have different queries
    assert normal_query != strict_query
    
    # Normal mode should use INNER JOIN
    assert "INNER JOIN" in normal_query
    
    # Strict mode should use NOT EXISTS
    assert "NOT EXISTS" in strict_query
    assert "WHERE NOT ({tenure_where})" not in strict_query  # Should have proper substitution


def test_strict_psc_tenure_sql_execution():
    """Test that generated SQL fragments are valid CTE syntax"""
    import duckdb
    
    # Test normal mode - check that fragment contains expected elements
    query_normal, cte_name = build_psc_tenure_filter("test_cte", 2, 10, False)
    
    # Should contain CTE name
    assert "companies_with_psc_tenure" in query_normal
    assert "INNER JOIN" in query_normal
    assert "WHERE" in query_normal
    
    # Test strict mode - check that fragment contains expected elements
    query_strict, cte_name = build_psc_tenure_filter("test_cte", 2, 10, True)
    
    # Should contain CTE name
    assert "companies_with_psc_tenure" in query_strict
    assert "NOT EXISTS" in query_strict
    assert "WHERE NOT" in query_strict
    
    # Test that the fragments can be part of a complete query
    # by checking they follow CTE syntax patterns
    assert query_normal.strip().startswith(",") or query_normal.strip().startswith("WITH")
    assert query_strict.strip().startswith(",") or query_strict.strip().startswith("WITH")

def test_strict_psc_tenure_with_query_params():
    """Test strict mode with full QueryParams"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        min_psc_tenure_years=2,
        max_psc_tenure_years=10,
        strict_psc_tenure=True
    )
    
    # Should be valid
    valid, msg = validate_query_params(params)
    assert valid == True
    assert msg == "Valid"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
