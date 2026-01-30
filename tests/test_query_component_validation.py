"""
Comprehensive validation tests for query builder components
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query import (
    build_psc_age_filter, 
    build_psc_tenure_filter, 
    build_sic_filter,
    build_base_query,
    build_aggregates_query,
    build_radius_filter,
    build_final_select,
    build_query,
    QueryParams
)
import duckdb


class TestComponentFunctions:
    """Direct tests for individual query builder component functions"""
    
    def test_build_psc_age_filter_structure(self):
        """Test build_psc_age_filter function structure and comma placement"""
        query, cte_name = build_psc_age_filter("companies_in_radius", 50, 70)
        
        # Verify CTE name
        assert cte_name == "companies_with_psc_age"
        
        # Verify query structure
        assert "companies_with_psc_age AS (" in query
        assert "approximate_age >= 50" in query
        assert "approximate_age <= 70" in query
        assert "FROM companies_in_radius c" in query
        
        # Critical: Verify comma at start (the bug that was fixed)
        assert query.strip().startswith(",")
    
    def test_build_psc_age_filter_edge_cases(self):
        """Test edge cases for PSC age filter"""
        # Test with only min age (max is default 120)
        query1, cte1 = build_psc_age_filter("test_cte", 50, 120)
        assert "approximate_age >= 50" in query1
        assert "approximate_age <=" not in query1
        
        # Test with only max age (min is default 16)
        query2, cte2 = build_psc_age_filter("test_cte", 16, 70)
        assert "approximate_age <= 70" in query2
        assert "approximate_age >=" not in query2
        
        # Test with default values (should have 1=1 condition)
        query3, cte3 = build_psc_age_filter("test_cte", 16, 120)
        assert "1=1" in query3
    
    def test_build_sic_filter_structure(self):
        """Test build_sic_filter function structure"""
        query, cte_name = build_sic_filter("companies_in_radius", [62012, 62020])
        
        assert cte_name == "companies_with_sic"
        assert "companies_with_sic AS (" in query
        assert "62012" in query
        assert "62020" in query
        assert query.strip().startswith(",")  # Should start with comma
    
    def test_build_psc_tenure_filter_modes(self):
        """Test build_psc_tenure_filter function in both modes"""
        # Test normal mode (ANY PSC meets criteria)
        query1, cte1 = build_psc_tenure_filter("test_cte", 5, 10, strict_mode=False)
        assert cte1 == "companies_with_psc_tenure"
        assert query1.strip().startswith(",")
        assert "INNER JOIN" in query1
        
        # Test strict mode (ALL PSCs must meet criteria)
        query2, cte2 = build_psc_tenure_filter("test_cte", 5, 10, strict_mode=True)
        assert cte2 == "companies_with_psc_tenure"
        assert query2.strip().startswith(",")
        assert "NOT EXISTS" in query2


class TestSQLSyntaxValidation:
    """Tests that validate SQL syntax of generated queries"""
    
    @pytest.mark.parametrize("test_name,params", [
        ("basic_query", QueryParams(postcode="SW1A 1AA", radius_miles=10.0)),
        ("query_with_psc_age", QueryParams(postcode="SW1A 1AA", radius_miles=10.0, min_psc_age=50, max_psc_age=70)),
        ("query_with_sic_codes", QueryParams(postcode="SW1A 1AA", radius_miles=10.0, sic_codes=[62012, 62020])),
        ("query_with_psc_tenure", QueryParams(postcode="SW1A 1AA", radius_miles=10.0, min_psc_tenure_years=5, max_psc_tenure_years=10)),
        ("complex_query", QueryParams(
            postcode="SW1A 1AA", 
            radius_miles=10.0,
            sic_codes=[62012],
            min_psc_age=50,
            max_psc_age=70,
            min_psc_tenure_years=5,
            max_psc_tenure_years=10,
            account_categories=["MICRO ENTITY"]
        )),
    ])
    def test_sql_syntax_valid(self, test_name, params):
        """Test that generated queries have valid SQL syntax"""
        query = build_query(51.5, -0.1, params)
        
        # Validate SQL syntax using DuckDB
        con = duckdb.connect()
        try:
            # This will raise an exception if SQL syntax is invalid
            con.execute("EXPLAIN " + query)
        finally:
            con.close()


class TestCTEStructureValidation:
    """Tests that validate CTE structure and separation"""
    
    def test_cte_comma_separation(self):
        """Test that all CTEs are properly separated with commas"""
        params = QueryParams(
            postcode="SW1A 1AA",
            radius_miles=10.0,
            sic_codes=[62012],
            min_psc_age=50,
            max_psc_age=70
        )
        
        query = build_query(51.5, -0.1, params)
        
        # Find all CTE definitions that should be comma-separated
        cte_patterns = [
            "sic_aggregates AS (",
            "psc_aggregates AS (",
            "companies_enriched AS (",
            "companies_in_radius AS (",
            "companies_with_sic AS (",
            "companies_with_psc_age AS ("
        ]
        
        for cte_pattern in cte_patterns:
            if cte_pattern in query:
                cte_start = query.find(cte_pattern)
                # Extract context before the CTE
                context_before = query[:cte_start]
                
                if "WITH" not in context_before:  # Not the first CTE
                    # Should be preceded by comma
                    lines_before = context_before.split('\n')
                    last_non_empty_line = ""
                    for line in reversed(lines_before):
                        if line.strip():
                            last_non_empty_line = line
                            break
                    
                    assert "," in last_non_empty_line, f"CTE '{cte_pattern}' should be preceded by comma"


class TestComponentSQLValidation:
    """Tests that validate individual components can form valid SQL"""
    
    def test_psc_age_filter_sql_validity(self):
        """Test that build_psc_age_filter generates valid SQL"""
        query, cte_name = build_psc_age_filter("companies_in_radius", 50, 70)
        
        # Create a complete test query with proper table references
        full_test_query = f"""
        WITH companies_in_radius AS (
            SELECT '123' AS CompanyNumber, 'ABC' AS company_number
        ),
        psc_data AS (
            SELECT 'ABC' AS company_number, 55 AS approximate_age
        )
        {query}
        SELECT COUNT(*) FROM {cte_name}
        """
        
        # Validate SQL syntax
        con = duckdb.connect()
        try:
            con.execute("EXPLAIN " + full_test_query)
        finally:
            con.close()

    def test_sic_filter_sql_validity(self):
        """Test that build_sic_filter generates valid SQL"""
        query, cte_name = build_sic_filter("companies_in_radius", [62012, 62020])
        
        # Create a complete test query
        full_test_query = f"""
        WITH companies_in_radius AS (
            SELECT '123' AS CompanyNumber
        ),
        sic_data AS (
            SELECT '123' AS CompanyNumber, 62012 AS sic_code, 1 AS sic_position, 'Test' AS sic_description
        )
        {query}
        SELECT COUNT(*) FROM {cte_name}
        """
        
        # Validate SQL syntax
        con = duckdb.connect()
        try:
            con.execute("EXPLAIN " + full_test_query)
        finally:
            con.close()