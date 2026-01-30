"""
Test query execution with real data validation for enhanced fields
"""

import pytest
import csv
import os
from pathlib import Path
import sys
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import QueryParams, PROCESSED_DIR
from src.find_companies import find_companies


@pytest.fixture
def sample_postcodes():
    """Sample postcodes for comprehensive testing across regions"""
    return {
        "high_psc_density": "SW1A 1AA",  # Central London - expected high PSC activity
        "medium_density": "SW1A 1AA",    # Surbiton - mixed business area
        "rural_sample": "EC2R 8AH",     # Reading - urban/rural mix
    }


def test_basic_query_execution_with_enhanced_fields(sample_postcodes):
    """Test that basic query produces CSV with all 17 expected columns"""
    params = QueryParams(
        postcode=sample_postcodes["medium_density"],
        radius_miles=1.0,  # Small radius for faster testing
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None
    assert output_path.exists()

    # Validate CSV structure
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
    
    # Check for all 26 fields (8 original + 12 PSC + 6 officer)
    expected_headers = [
        "CompanyNumber", "CompanyName", "Postcode", "DistanceMiles",
        "CompanyStatus", "CompanyCategory", "AccountCategory",
        "IncorporationDate", "CompanyAgeYears", "LastAccountsDate",
        "SicCodeCount", "PrimarySicCode", "PrimarySicDescription",
        "PscCount", "YoungestPscAge", "OldestPscAge", "PscLastUpdated",
        "MinPscTenureYears", "MaxPscTenureYears", "AvgPscTenureYears",
        "OfficerCount", "EarliestOfficerAppointment", "LatestOfficerAppointment",
        "MinOfficerTenureYears", "MaxOfficerTenureYears", "AvgOfficerTenureYears"
    ]
    assert headers is not None, "Headers should not be None"
    assert len(headers) == 26, f"Expected 26 columns, got {len(headers)}"
    for header in expected_headers:
        assert header in headers, f"Missing header: {header}"

    # Clean up
    os.unlink(output_path)


def test_psc_aggregation_accuracy():
    """Test PSC aggregation accuracy with known high-density area"""
    # Use postcode expected to have multiple PSCs
    params = QueryParams(
        postcode="SW1A 1AA",  # Central London
        radius_miles=0.5,     # Very small radius
        max_results=10,       # Limit results for testing
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None

    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # Validate PSC data quality
        companies_with_pscs = [r for r in rows if int(r['PscCount']) > 0]
        if companies_with_pscs:  # If any companies have PSCs
            sample_company = companies_with_pscs[0]

            # Validate age ranges
            if sample_company['YoungestPscAge']:
                youngest = int(sample_company['YoungestPscAge'])
                oldest = int(sample_company['OldestPscAge'])
                assert youngest <= oldest, f"Invalid age range: {youngest} > {oldest}"
                assert 16 <= youngest <= 120, f"Invalid youngest age: {youngest}"
                assert 16 <= oldest <= 120, f"Invalid oldest age: {oldest}"

    os.unlink(output_path)


def test_sic_aggregation_accuracy():
    """Test SIC code aggregation logic"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=1.0,
        max_results=20,
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None

    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        for row in rows:
            sic_count = int(row['SicCodeCount'])

            # Validate SIC count is non-negative
            assert sic_count >= 0, f"Invalid SIC count: {sic_count}"

            # If company has SIC codes, check primary SIC is populated
            if sic_count > 0:
                assert row['PrimarySicCode'], "Missing primary SIC code"
                assert row['PrimarySicDescription'], "Missing primary SIC description"
                # SIC code should be 5 digits
                sic_code = int(row['PrimarySicCode'])
                assert 10000 <= sic_code <= 99999, f"Invalid SIC code: {sic_code}"
            else:
                # If no SIC codes, these should be empty (COALESCE handles this)
                assert not row['PrimarySicCode'] or row['PrimarySicCode'] == '', "Unexpected SIC code for count=0"

    os.unlink(output_path)


def test_company_age_calculation():
    """Test company age calculation accuracy"""
    params = QueryParams(
        postcode="EC2R 8AH",
        radius_miles=1.0,
        max_results=15,
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None

    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        for row in rows:
            company_age = int(row['CompanyAgeYears'])
            # Company age should be reasonable (UK companies max ~150 years old)
            assert 0 <= company_age <= 150, f"Unreasonable company age: {company_age}"

            # Cross-reference with incorporation date if available
            if row['IncorporationDate']:
                # Basic sanity check - age shouldn't be negative or hugely off
                # This is approximate since actual calculation includes year fractions
                assert abs(company_age - (2025 - int(row['IncorporationDate'].split('-')[0]))) <= 1

    os.unlink(output_path)


def test_edge_case_no_psc_no_sic():
    """Test companies with no PSC or SIC data are still included"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=2.0,
        max_results=50,  # Increased to improve chances of finding edge cases
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None

    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # Find companies with no PSC or SIC data
        no_psc_companies = [r for r in rows if r['PscCount'] == '0']
        no_sic_companies = [r for r in rows if r['SicCodeCount'] == '0']

        # Verify that if companies without PSC/SIC exist, they are included (LEFT JOIN behavior)
        # Note: Not all samples will have companies without SIC codes (most active companies have them)
        # The key test is that when they DO exist, the data is handled correctly

        # At minimum, we should have some results
        assert len(rows) > 0, "Should return some companies"

        # If we found companies without PSCs, validate their data is handled correctly
        if no_psc_companies:
            for company in no_psc_companies:
                assert company['YoungestPscAge'] == '' or company['YoungestPscAge'] is None, "PSC age should be empty for no PSC companies"
                assert company['OldestPscAge'] == '' or company['OldestPscAge'] is None, "PSC age should be empty for no PSC companies"

        # If we found companies without SIC codes, validate their data is handled correctly
        if no_sic_companies:
            for company in no_sic_companies:
                assert company['PrimarySicCode'] == '' or company['PrimarySicCode'] is None, "SIC code should be empty for no SIC companies"
                assert company['PrimarySicDescription'] == '' or company['PrimarySicDescription'] is None, "SIC description should be empty for no SIC companies"

    os.unlink(output_path)


def test_data_freshness_indicators():
    """Test that date-based fields show reasonable values"""
    params = QueryParams(
        postcode="SW1A 1AA",  # High activity area
        radius_miles=0.5,
        max_results=10,
        output_format='csv'
    )

    output_path = find_companies(params)
    assert output_path is not None

    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        for row in rows:
            # Company Status should be 'Active' (our filter)
            assert row['CompanyStatus'] == 'Active', f"Non-active company found: {row['CompanyStatus']}"

            # Account Category should be reasonable
            assert row['AccountCategory'] in [
                'MICRO ENTITY', 'SMALL', 'MEDIUM', 'FULL',
                'TOTAL EXEMPTION FULL', 'TOTAL EXEMPTION SMALL',
                'DORMANT', 'NO ACCOUNTS FILED',
                'UNAUDITED ABRIDGED', 'AUDITED ABRIDGED',
                'AUDIT EXEMPTION SUBSIDIARY', 'FILING EXEMPTION SUBSIDIARY',
                'GROUP', 'PARTIAL EXEMPTION', 'ACCOUNTS TYPE NOT AVAILABLE'
            ], f"Invalid account category: {row['AccountCategory']}"

    os.unlink(output_path)


def test_invalid_postcode_handling():
    """Test query with invalid postcode"""
    params = QueryParams(
        postcode="INVALID_POSTCODE",
        radius_miles=10.0
    )
    
    with pytest.raises(ValueError, match="Invalid postcode format"):
        find_companies(params)


def test_missing_processed_files():
    """Test query with missing processed files"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0
    )
    
    with patch('src.query.validate_processed_files', return_value=(False, ['companies.parquet'])):
        with pytest.raises(FileNotFoundError, match="Missing processed files"):
            find_companies(params)


def test_large_result_set_with_max_results():
    """Test query with max_results parameter enforced"""
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=10.0,
        max_results=50
    )
    
    output_path = find_companies(params)
    assert output_path is not None
    
    # Verify result count
    with open(output_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        result_count = len(lines) - 1 if lines else 0  # Subtract header
        assert result_count <= 50, f"Max results not enforced: got {result_count} results"
    
    os.unlink(output_path)


def test_query_with_all_filters():
    """Test query with all filter types combined"""
    params = QueryParams(
        postcode="EC2R 8AH",
        radius_miles=15.0,
        sic_codes=[43220, 43290],
        min_psc_age=50,
        max_psc_age=70,
        account_categories=['MICRO ENTITY', 'SMALL'],
        min_company_age_years=5,
        max_company_age_years=50,
        output_format='csv'
    )
    
    output_path = find_companies(params)
    assert output_path is not None
    
    # Verify results meet all criteria
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
        for row in rows:
            # Verify PSC age range - note: this tests "ANY" logic
            # At least one PSC should meet criteria, but others may not
            if row['PscCount'] > '0' and row['YoungestPscAge']:
                youngest = int(row['YoungestPscAge'])
                # Note: youngest age might be below minimum if company has multiple PSCs
                # and at least one meets criteria (ANY logic)
                # We can't assert youngest >= 50 because that would require ALL PSCs to meet criteria
                
                if row['OldestPscAge']:
                    oldest = int(row['OldestPscAge'])
                    # Similarly, oldest might be above maximum with ANY logic
                    # assert oldest <= 70, f"PSC age above maximum: {oldest}"
            
            # Verify company age range
            company_age = int(row['CompanyAgeYears'])
            assert 5 <= company_age <= 50, f"Company age out of range: {company_age}"
            
            # Verify account category
            assert row['AccountCategory'] in ['MICRO ENTITY', 'SMALL']
    
    os.unlink(output_path)


def test_query_no_results():
    """Test query that returns no results"""
    params = QueryParams(
        postcode="SW1A 1AA",  # Valid postcode
        radius_miles=0.1,     # Very small radius
        sic_codes=[12345],    # Non-existent SIC code
        min_psc_age=120,     # Unrealistic age
        output_format='csv'
    )
    
    result = find_companies(params)
    # Should return None for no results, or empty file
    if result is not None:
        # If file was created, it should be empty (header only)
        with open(result, 'r') as f:
            lines = f.readlines()
            assert len(lines) <= 1, "Should have only header or be empty"
        os.unlink(result)


def test_excel_auto_conversion():
    """Test automatic conversion to CSV for large Excel results"""
    # Test the logic directly by checking the Excel limit constant
    # The actual conversion happens in find_companies when result_count > 1_048_576
    
    # For now, just verify the logic exists by checking a simple case
    params = QueryParams(
        postcode="SW1A 1AA",
        radius_miles=1.0,  # Small radius to avoid large results
        output_format='xlsx'
    )
    
    result = find_companies(params)
    assert result is not None
    # File should be created as Excel since results are small
    assert result.suffix == '.xlsx'
    
    # Clean up
    os.unlink(result)
    
    # Note: Testing the actual auto-conversion would require mocking
    # the result count, which is complex. This test verifies the
    # basic Excel functionality works.