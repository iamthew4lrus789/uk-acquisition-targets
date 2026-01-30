"""
Test for the bug where CompanyStatus shows Active yet AccountCategory says dormant
"""

import pytest
import csv
import os
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import QueryParams
from src.find_companies import find_companies


def test_dormant_companies_should_not_be_active():
    """Test that companies with AccountCategory='DORMANT' should not have CompanyStatus='Active'"""
    
    # Use a postcode that might have dormant companies
    params = QueryParams(
        postcode="SW1A 1AA",  # Central London
        radius_miles=1.0,
        output_format='csv'
    )
    
    output_path = find_companies(params)
    assert output_path is not None
    assert output_path.exists()
    
    dormant_companies_with_active_status = []
    
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check for the bug: CompanyStatus is Active but AccountCategory is DORMANT
            if row['CompanyStatus'] == 'Active' and row['AccountCategory'] == 'DORMANT':
                dormant_companies_with_active_status.append(row)
    
    # Clean up
    os.unlink(output_path)
    
    # The bug exists if we found any companies with this inconsistency
    if dormant_companies_with_active_status:
        print(f"\n❌ BUG FOUND: {len(dormant_companies_with_active_status)} companies have CompanyStatus='Active' but AccountCategory='DORMANT'")
        for company in dormant_companies_with_active_status[:3]:  # Show first 3 examples
            print(f"  Company: {company['CompanyName']} ({company['CompanyNumber']})")
            print(f"    Status: {company['CompanyStatus']}")
            print(f"    Account Category: {company['AccountCategory']}")
        
        # This assertion will fail if the bug exists
        assert False, f"Found {len(dormant_companies_with_active_status)} companies with CompanyStatus='Active' but AccountCategory='DORMANT'"
    else:
        print("✓ No dormant companies with active status found")


def test_dormant_account_category_filtering():
    """Test that when filtering by account categories excluding DORMANT, no dormant companies are returned"""
    
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=1.0,
        account_categories=['MICRO ENTITY', 'SMALL'],  # Explicitly exclude DORMANT
        output_format='csv'
    )
    
    output_path = find_companies(params)
    assert output_path is not None
    
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # When we explicitly filter for non-dormant categories,
            # we should NOT get any companies with AccountCategory='DORMANT'
            assert row['AccountCategory'] != 'DORMANT', \
                f"Company {row['CompanyName']} has AccountCategory='DORMANT' despite filtering for non-dormant categories"
    
    os.unlink(output_path)


def test_explicit_dormant_filtering():
    """Test that when explicitly requesting DORMANT account category, dormant companies are returned"""
    
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=1.0,
        account_categories=['DORMANT'],  # Explicitly request DORMANT companies
        output_format='csv'
    )
    
    output_path = find_companies(params)
    assert output_path is not None
    
    dormant_found = False
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['AccountCategory'] == 'DORMANT':
                dormant_found = True
                # These should still have Active status (data quality issue in source data)
                assert row['CompanyStatus'] == 'Active', \
                    f"Dormant company {row['CompanyName']} should still have Active status in our filtered results"
                break
    
    os.unlink(output_path)
    
    # We should find at least some dormant companies when explicitly requested
    assert dormant_found, "No dormant companies found when explicitly requesting DORMANT account category"
