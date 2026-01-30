#!/usr/bin/env python3
"""
Test PSC-Officer integration with career progression tracking
"""
import pytest
from src.query import build_psc_tenure_filter


def test_psc_officer_integration_basic():
    """Test basic PSC-Officer integration"""
    query, cte_name = build_psc_tenure_filter(
        "companies_in_radius", 
        min_tenure=10, 
        max_tenure=20
    )
    
    # Should reference officer appointment data for career progression tracking
    assert "officers.parquet" in query, \
        "PSC-Officer integration not implemented yet"
    assert "appointment_date" in query, \
        "Officer appointment date not used for career progression"
    assert "career progression" in query.lower(), \
        "Career progression tracking not mentioned in query"


def test_career_progression_tracking():
    """Test that career progression is tracked"""
    query, cte_name = build_psc_tenure_filter(
        "companies_in_radius", 
        min_tenure=15,
        max_tenure=30
    )
    
    # Should track career progression for accurate tenure
    assert "career progression" in query.lower(), \
        "Career progression tracking not mentioned"
    assert "effective appointment dates" in query.lower(), \
        "Effective appointment dates not used for career progression"
    assert "appointment_date" in query, \
        "Officer appointment date not used for tenure calculation"


if __name__ == "__main__":
    # Run tests to confirm they fail (expected before implementation)
    try:
        test_psc_officer_integration_basic()
        print("❌ Test passed unexpectedly - feature may already be implemented")
    except AssertionError as e:
        print(f"✅ Test failed as expected: {e}")
    
    try:
        test_career_progression_tracking()
        print("❌ Test passed unexpectedly - feature may already be implemented")
    except AssertionError as e:
        print(f"✅ Test failed as expected: {e}")