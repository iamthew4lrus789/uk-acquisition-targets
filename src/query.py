#!/usr/bin/env python3
"""
query.py - Query Companies House data
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass

import duckdb

# ============================================================================
# CONFIGURATION
# ============================================================================

PROCESSED_DIR = Path("processed")

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class QueryParams:  # pylint: disable=too-many-instance-attributes
    """User query parameters"""
    # REQUIRED fields
    postcode: str
    radius_miles: float

    # OPTIONAL fields with defaults
    company_status: str = 'Active'
    account_categories: Optional[List[str]] = None
    sic_codes: Optional[List[int]] = None
    max_company_age_years: Optional[int] = None
    min_company_age_years: Optional[int] = None
    min_psc_age: Optional[int] = None
    max_psc_age: Optional[int] = None
    min_psc_tenure_years: Optional[int] = None
    max_psc_tenure_years: Optional[int] = None
    strict_psc_tenure: Optional[bool] = None
    output_format: str = 'csv'
    output_path: Optional[Path] = None
    max_results: Optional[int] = None

# ============================================================================
# VALIDATION
# ============================================================================

def validate_query_params(params: QueryParams) -> tuple[bool, str]:
    # pylint: disable=too-many-return-statements,too-many-branches
    """
    Validate query parameters before execution

    Returns:
        (is_valid, error_message)
    """
    # Check required fields are present (should not be None after dataclass update)
    if params.postcode is None:
        return False, "Postcode is required"
    if params.radius_miles is None:
        return False, "Radius is required"

    # Postcode format (basic check)
    postcode = params.postcode.strip()
    if len(postcode) < 5 or len(postcode) > 8:
        return False, f"Invalid postcode format: '{params.postcode}'"

    # Radius bounds and type check
    if not isinstance(params.radius_miles, (int, float)):
        return False, "Radius must be a number"
    if params.radius_miles <= 0:
        return False, "Radius must be greater than 0"
    if params.radius_miles > 500:
        return False, "Radius must be ≤ 500 miles"

    # Company status whitelist (prevent SQL injection)
    VALID_STATUSES = ['Active', 'Dissolved', 'Liquidation', 'Administration']
    if params.company_status not in VALID_STATUSES:
        return False, f"Invalid company status. Must be one of: {VALID_STATUSES}"

    # SIC codes format (stronger validation for SQL injection prevention)
    if params.sic_codes is not None:
        if not params.sic_codes:
            return False, "SIC codes list cannot be empty"
        for code in params.sic_codes:
            if not isinstance(code, int):
                return False, f"SIC code must be integer: {code}"
            if code < 0 or code > 99999:
                return False, f"SIC code out of range (0-99999): {code}"
            if not 10000 <= code <= 99999:
                return False, f"SIC code must be 5 digits: {code}"

    # Account categories
    valid_categories = [
        'MICRO ENTITY', 'SMALL', 'MEDIUM', 'FULL',
        'TOTAL EXEMPTION FULL', 'TOTAL EXEMPTION SMALL',
        'DORMANT', 'NO ACCOUNTS FILED',
        'UNAUDITED ABRIDGED', 'AUDITED ABRIDGED',
        'AUDIT EXEMPTION SUBSIDIARY', 'FILING EXEMPTION SUBSIDIARY',
        'GROUP', 'PARTIAL EXEMPTION', 'ACCOUNTS TYPE NOT AVAILABLE'
    ]
    if params.account_categories is not None:
        for cat in params.account_categories:
            if cat not in valid_categories:
                return False, f"Invalid account category: '{cat}'"

    # Age bounds
    if params.min_psc_age is not None:
        if params.min_psc_age < 16:
            return False, "Minimum PSC age must be ≥ 16"
        if params.min_psc_age > 120:
            return False, "Minimum PSC age must be ≤ 120"

    if params.max_psc_age is not None:
        if params.max_psc_age < 16:
            return False, "Maximum PSC age must be ≥ 16"
        if params.max_psc_age > 120:
            return False, "Maximum PSC age must be ≤ 120"

    if (params.min_psc_age is not None and
        params.max_psc_age is not None and
        params.min_psc_age > params.max_psc_age):
        return False, "Minimum age cannot exceed maximum age"

    # PSC tenure bounds
    if params.min_psc_tenure_years is not None:
        if params.min_psc_tenure_years < 1:
            return False, "Minimum PSC tenure must be ≥ 1"
        if params.min_psc_tenure_years > 100:
            return False, "Minimum PSC tenure must be ≤ 100"

    if params.max_psc_tenure_years is not None:
        if params.max_psc_tenure_years < 1:
            return False, "Maximum PSC tenure must be ≥ 1"
        if params.max_psc_tenure_years > 100:
            return False, "Maximum PSC tenure must be ≤ 100"

    if (params.min_psc_tenure_years is not None and
        params.max_psc_tenure_years is not None and
        params.min_psc_tenure_years > params.max_psc_tenure_years):
        return False, "Minimum tenure cannot exceed maximum tenure"

    # Company age bounds
    if params.min_company_age_years is not None:
        if params.min_company_age_years < 0:
            return False, "Minimum company age must be ≥ 0"
        if params.min_company_age_years > 200:
            return False, "Minimum company age must be ≤ 200 years"

    if params.max_company_age_years is not None:
        if params.max_company_age_years < 0:
            return False, "Maximum company age must be ≥ 0"
        if params.max_company_age_years > 200:
            return False, "Maximum company age must be ≤ 200 years"

    if (params.min_company_age_years is not None and
        params.max_company_age_years is not None and
        params.min_company_age_years > params.max_company_age_years):
        return False, "Minimum company age cannot exceed maximum company age"

    # Output format
    if params.output_format not in ['csv', 'xlsx']:
        return False, f"Output format must be 'csv' or 'xlsx', not '{params.output_format}'"

    return True, "Valid"

def validate_processed_files() -> tuple[bool, list[str]]:
    """
    Check that all required processed files exist

    Returns:
        (all_exist, missing_files)
    """
    required = [
        'companies.parquet',
        'company_sic.parquet',
        'psc.parquet',
        'postcodes.parquet'
    ]

    missing = [f for f in required if not (PROCESSED_DIR / f).exists()]
    return (len(missing) == 0, missing)

def validate_postcode_exists(postcode: str) -> tuple[bool, str]:
    """
    Check if postcode exists in database (fail-fast validation)

    Returns:
        (exists, formatted_postcode_or_error_message)
    """
    try:
        con = duckdb.connect()
        normalized = postcode.upper().replace(' ', '')

        result = con.execute("""
            SELECT pcds FROM 'processed/postcodes.parquet'
            WHERE REPLACE(UPPER(pcds), ' ', '') = ?
            LIMIT 1
        """, [normalized]).fetchone()

        con.close()

        if result is None:
            return False, f"Postcode '{postcode}' not found in ONSPD database"

        return True, result[0]  # Return formatted postcode

    except Exception as e:
        return False, f"Error checking postcode: {e}"

# ============================================================================
# QUERY BUILDER - Modular Components
# ============================================================================

def build_base_query(target_lat: float, target_lon: float, params: QueryParams) -> str:
    """
    Build companies_with_coords CTE with coordinates and distance calculation

    Returns:
        SQL fragment for the base CTE
    """
    query = f"""    WITH
    companies_with_coords AS (
        SELECT
            c.CompanyNumber,
            c.CompanyName,
            c.RegAddressPostCode,
            c.CompanyStatus,
            c.CompanyCategory,
            c.AccountCategory,
            c.IncorporationDate,
            c.AccountsLastMadeUpDate,
            p.lat,
            p.long,
            -- Haversine distance formula (miles)
            3959 * 2 * ASIN(SQRT(
                POW(SIN(({target_lat} - p.lat) * PI()/180 / 2), 2) +
                COS({target_lat} * PI()/180) * COS(p.lat * PI()/180) *
                POW(SIN(({target_lon} - p.long) * PI()/180 / 2), 2)
            )) AS distance_miles
        FROM '{PROCESSED_DIR}/companies.parquet' c
        LEFT JOIN '{PROCESSED_DIR}/postcodes.parquet' p
            ON REPLACE(UPPER(c.RegAddressPostCode), ' ', '') =
               REPLACE(UPPER(p.pcds), ' ', '')
         WHERE c.CompanyStatus = '{params.company_status}'
           AND p.lat IS NOT NULL  -- Must have valid coordinates"""

     # Add account category filter if specified
    if params.account_categories:
        cat_list = "', '".join(params.account_categories)
        query += f"\n          AND c.AccountCategory IN ('{cat_list}')"
    else:
        # Exclude dormant companies when no specific account categories are requested
        # This ensures data quality: Active companies should not have DORMANT account category
        query += f"\n          AND (c.AccountCategory != 'DORMANT' OR c.AccountCategory IS NULL)"

    # Add company age filters if specified
    if params.min_company_age_years is not None:
        query += f"\n          AND (YEAR(CURRENT_DATE) - YEAR(c.IncorporationDate)) >= {params.min_company_age_years}"
    if params.max_company_age_years is not None:
        query += f"\n          AND (YEAR(CURRENT_DATE) - YEAR(c.IncorporationDate)) <= {params.max_company_age_years}"

    query += "\n    )"
    return query


def build_aggregates_query() -> str:
    """
    Build SIC and PSC aggregation CTEs

    Returns:
        SQL fragment for aggregation CTEs
    """
    return f""",

    -- Aggregate SIC data per company
    sic_aggregates AS (
        SELECT
            CompanyNumber,
            COUNT(DISTINCT sic_code) AS sic_count,
            MAX(CASE WHEN sic_position = 1 THEN sic_code ELSE NULL END) AS primary_sic_code,
            MAX(CASE WHEN sic_position = 1 THEN sic_description ELSE NULL END) AS primary_sic_description
        FROM '{PROCESSED_DIR}/company_sic.parquet'
        GROUP BY CompanyNumber
    ),

    -- Aggregate PSC data per company
    psc_aggregates AS (
        SELECT
            company_number,
            COUNT(*) AS psc_count,
            MIN(approximate_age) AS youngest_psc_age,
            MAX(approximate_age) AS oldest_psc_age,
            MAX(notified_on) AS psc_last_updated,
            MIN(YEAR(CURRENT_DATE) - YEAR(notified_on)) AS min_psc_tenure_years,
            MAX(YEAR(CURRENT_DATE) - YEAR(notified_on)) AS max_psc_tenure_years,
            AVG(YEAR(CURRENT_DATE) - YEAR(notified_on)) AS avg_psc_tenure_years
        FROM '{PROCESSED_DIR}/psc.parquet'
        GROUP BY company_number
    ),

    -- Aggregate Officer data for career progression tracking
    officer_aggregates AS (
        SELECT
            company_number,
            COUNT(*) AS officer_count,
            MIN(appointment_date) AS earliest_officer_appointment,
            MAX(appointment_date) AS latest_officer_appointment,
            -- Note: appointment_date is stored as YYYYMMDD format (BIGINT), extract year from first 4 digits
            MIN(YEAR(CURRENT_DATE) - CAST(SUBSTR(CAST(appointment_date AS VARCHAR), 1, 4) AS INTEGER)) AS min_officer_tenure_years,
            MAX(YEAR(CURRENT_DATE) - CAST(SUBSTR(CAST(appointment_date AS VARCHAR), 1, 4) AS INTEGER)) AS max_officer_tenure_years,
            AVG(YEAR(CURRENT_DATE) - CAST(SUBSTR(CAST(appointment_date AS VARCHAR), 1, 4) AS INTEGER)) AS avg_officer_tenure_years
        FROM '{PROCESSED_DIR}/officers.parquet'
        WHERE appointment_date IS NOT NULL
        GROUP BY company_number
    ),

    -- Join all aggregations
    companies_enriched AS (
        SELECT
            c.*,
            COALESCE(s.sic_count, 0) AS sic_count,
            s.primary_sic_code,
            s.primary_sic_description,
            COALESCE(p.psc_count, 0) AS psc_count,
            p.youngest_psc_age,
            p.oldest_psc_age,
            p.psc_last_updated,
            p.min_psc_tenure_years,
            p.max_psc_tenure_years,
            p.avg_psc_tenure_years,
            COALESCE(o.officer_count, 0) AS officer_count,
            o.earliest_officer_appointment,
            o.latest_officer_appointment,
            o.min_officer_tenure_years,
            o.max_officer_tenure_years,
            o.avg_officer_tenure_years
        FROM companies_with_coords c
        LEFT JOIN sic_aggregates s ON c.CompanyNumber = s.CompanyNumber
        LEFT JOIN psc_aggregates p ON c.CompanyNumber = p.company_number
        LEFT JOIN officer_aggregates o ON c.CompanyNumber = o.company_number
    )"""


def build_radius_filter(radius_miles: float) -> str:
    """
    Build radius filtering CTE

    Returns:
        SQL fragment for radius filter
    """
    return f""",

    -- Filter by radius
    companies_in_radius AS (
        SELECT * FROM companies_enriched
        WHERE distance_miles <= {radius_miles}
    )"""


def build_sic_filter(current_cte: str, sic_codes: List[int]) -> tuple[str, str]:
    """
    Build SIC code filter CTE

    Args:
        current_cte: Name of the CTE to filter from
        sic_codes: List of SIC codes to filter by

    Returns:
        (query_fragment, new_cte_name)
    """
    sic_list = ','.join(str(code) for code in sic_codes)
    query = f""",

    companies_with_sic AS (
        SELECT DISTINCT c.*
        FROM {current_cte} c
        INNER JOIN '{PROCESSED_DIR}/company_sic.parquet' s
            ON c.CompanyNumber = s.CompanyNumber
        WHERE s.sic_code IN ({sic_list})
    )"""
    return query, "companies_with_sic"


def build_psc_age_filter(current_cte: str, min_age: int, max_age: int) -> tuple[str, str]:
    """
    Build PSC age filter CTE

    Args:
        current_cte: Name of the CTE to filter from
        min_age: Minimum PSC age (or 16 if not specified)
        max_age: Maximum PSC age (or 120 if not specified)

    Returns:
        (query_fragment, new_cte_name)
    """
    age_conditions = []
    if min_age > 16:  # Only filter if not default
        age_conditions.append(f"p.approximate_age >= {min_age}")
    if max_age < 120:  # Only filter if not default
        age_conditions.append(f"p.approximate_age <= {max_age}")

    age_where = " AND ".join(age_conditions) if age_conditions else "1=1"

    query = f""" ,

    companies_with_psc_age AS (
        SELECT DISTINCT c.*
        FROM {current_cte} c
        INNER JOIN '{PROCESSED_DIR}/psc.parquet' p
            ON c.CompanyNumber = p.company_number
        WHERE {age_where}
    )"""
    return query, "companies_with_psc_age"


def build_psc_tenure_filter(current_cte: str, min_tenure: int, max_tenure: int, strict_mode: bool = False) -> tuple[str, str]:
    """
    Build PSC tenure filter CTE with Officer integration for career progression tracking

    Args:
        current_cte: Name of the CTE to filter from
        min_tenure: Minimum PSC tenure in years (or 1 if not specified)
        max_tenure: Maximum PSC tenure in years (or 100 if not specified)
        strict_mode: If True, ALL PSCs must meet criteria. If False, ANY PSC must meet criteria.

    Returns:
        (query_fragment, new_cte_name)
    """
    tenure_conditions = []
    if min_tenure > 1:  # Only filter if not default
        tenure_conditions.append(f"(YEAR(CURRENT_DATE) - YEAR(p.notified_on)) >= {min_tenure}")
    if max_tenure < 100:  # Only filter if not default
        tenure_conditions.append(f"(YEAR(CURRENT_DATE) - YEAR(p.notified_on)) <= {max_tenure}")

    tenure_where = " AND ".join(tenure_conditions) if tenure_conditions else "1=1"

    if strict_mode:
        # Strict mode: ALL PSCs must meet criteria (no PSCs violate the criteria)
        # With Officer integration for career progression tracking
        query = f""" ,

    companies_with_psc_tenure AS (
        SELECT DISTINCT c.*
        FROM {current_cte} c
        WHERE NOT EXISTS (
            SELECT 1
            FROM '{PROCESSED_DIR}/psc.parquet' p
            WHERE c.CompanyNumber = p.company_number
            AND NOT ({tenure_where})
        )
        -- Officer integration: ensure career progression is tracked
        AND EXISTS (
            SELECT 1
            FROM '{PROCESSED_DIR}/officers.parquet' o
            WHERE c.CompanyNumber = o.company_number
            AND o.appointment_date IS NOT NULL
        )
    )"""
    else:
        # Normal mode: At least ONE PSC must meet criteria
        # With Officer integration for career progression tracking
        query = f""" ,

    companies_with_psc_tenure AS (
        SELECT DISTINCT c.*
        FROM {current_cte} c
        INNER JOIN '{PROCESSED_DIR}/psc.parquet' p
            ON c.CompanyNumber = p.company_number
        WHERE {tenure_where}
        -- Officer integration: track career progression using effective appointment dates
        AND EXISTS (
            SELECT 1
            FROM '{PROCESSED_DIR}/officers.parquet' o
            WHERE c.CompanyNumber = o.company_number
            AND o.appointment_date IS NOT NULL
            -- Use officer appointment date for more accurate career progression tracking
            -- Note: appointment_date is stored as YYYYMMDD format (BIGINT), extract year from first 4 digits
            AND (YEAR(CURRENT_DATE) - CAST(SUBSTR(CAST(o.appointment_date AS VARCHAR), 1, 4) AS INTEGER)) BETWEEN {min_tenure} AND {max_tenure}
        )
    )"""
    
    return query, "companies_with_psc_tenure"


def build_final_select(current_cte: str) -> str:
    """
    Build final SELECT with output columns

    Args:
        current_cte: Name of the final CTE to select from

    Returns:
        SQL fragment for final SELECT
    """
    return f"""

         SELECT
        CompanyNumber,
        CompanyName,
        RegAddressPostCode AS Postcode,
        ROUND(distance_miles, 2) AS DistanceMiles,
        CompanyStatus,
        CompanyCategory,
        AccountCategory,
        IncorporationDate,
        (YEAR(CURRENT_DATE) - YEAR(IncorporationDate)) AS CompanyAgeYears,
        AccountsLastMadeUpDate AS LastAccountsDate,
        sic_count AS SicCodeCount,
        primary_sic_code AS PrimarySicCode,
         primary_sic_description AS PrimarySicDescription,
         psc_count AS PscCount,
         youngest_psc_age AS YoungestPscAge,
         oldest_psc_age AS OldestPscAge,
         psc_last_updated AS PscLastUpdated,
         min_psc_tenure_years AS MinPscTenureYears,
         max_psc_tenure_years AS MaxPscTenureYears,
         avg_psc_tenure_years AS AvgPscTenureYears,
         officer_count AS OfficerCount,
         earliest_officer_appointment AS EarliestOfficerAppointment,
         latest_officer_appointment AS LatestOfficerAppointment,
         min_officer_tenure_years AS MinOfficerTenureYears,
         max_officer_tenure_years AS MaxOfficerTenureYears,
         avg_officer_tenure_years AS AvgOfficerTenureYears
    FROM {current_cte}
    ORDER BY distance_miles ASC"""


def build_query(target_lat: float, target_lon: float, params: QueryParams) -> str:
    """
    Build complete SQL query by composing modular components

    Query structure:
    1. Base CTE with coordinates and distance (Haversine formula)
    2. Aggregate SIC and PSC data
    3. Filter by radius
    4. Optionally filter by SIC codes
    5. Optionally filter by PSC age
    6. Optionally filter by PSC tenure
    7. Final SELECT with all output columns

    Args:
        target_lat: Target latitude
        target_lon: Target longitude
        params: Query parameters

    Returns:
        Complete SQL query string
    """
    parts = []

    # Base query with coordinates
    parts.append(build_base_query(target_lat, target_lon, params))

    # Aggregations
    parts.append(build_aggregates_query())

    # Radius filter
    parts.append(build_radius_filter(params.radius_miles))

    current_cte = "companies_in_radius"

    # Optional SIC filter
    if params.sic_codes:
        sic_part, current_cte = build_sic_filter(current_cte, params.sic_codes)
        parts.append(sic_part)

    # Optional PSC age filter
    if params.min_psc_age is not None or params.max_psc_age is not None:
        psc_part, current_cte = build_psc_age_filter(
            current_cte,
            params.min_psc_age or 16,
            params.max_psc_age or 120
        )
        parts.append(psc_part)

    # Optional PSC tenure filter
    if params.min_psc_tenure_years is not None or params.max_psc_tenure_years is not None:
        tenure_part, current_cte = build_psc_tenure_filter(
            current_cte,
            params.min_psc_tenure_years or 1,
            params.max_psc_tenure_years or 100,
            params.strict_psc_tenure or False
        )
        parts.append(tenure_part)

    # Final select
    parts.append(build_final_select(current_cte))

    return "\n".join(parts)

# ============================================================================
# DEPRECATED: Old monolithic build_query() implementation removed
# ============================================================================
# The build_query() function has been refactored into smaller, testable components.
# See build_base_query(), build_aggregates_query(), build_radius_filter(),
# build_sic_filter(), build_psc_age_filter(), and build_final_select() above.


# ============================================================================
# QUERY EXECUTION
# ============================================================================

def find_companies(params: QueryParams) -> Optional[Path]:
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """
    Execute query and return path to output file

    Returns:
        Path to output file, or None if no results
    """
    # Validate parameters
    valid, error_msg = validate_query_params(params)
    if not valid:
        raise ValueError(f"Invalid query parameters: {error_msg}")

    # Check processed files exist
    files_exist, missing = validate_processed_files()
    if not files_exist:
        raise FileNotFoundError(
            f"Missing processed files: {', '.join(missing)}. "
            f"Run setup.py first."
        )

    print("=" * 80)
    print("  Companies House Query")
    print("=" * 80)
    start_time = datetime.now()
    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    con = duckdb.connect()

    try:
        # ====================================================================
        # STEP 1: Validate and lookup target postcode (fail-fast)
        # ====================================================================

        print(f"\n📍 Validating postcode: {params.postcode}")

        # Fail-fast postcode validation
        exists, formatted_or_error = validate_postcode_exists(params.postcode)
        if not exists:
            raise ValueError(formatted_or_error)

        print(f"  ✓ Postcode validated: {formatted_or_error}")

        # Now get full coordinates
        normalized_postcode = params.postcode.upper().replace(' ', '')
        coords = con.execute("""
            SELECT lat, long, pcds
            FROM 'processed/postcodes.parquet'
            WHERE REPLACE(UPPER(pcds), ' ', '') = ?
            LIMIT 1
        """, [normalized_postcode]).fetchone()

        target_lat, target_lon, formatted_postcode = coords
        print(f"  ✓ Coordinates: ({target_lat:.6f}, {target_lon:.6f})")

        # ====================================================================
        # STEP 2: Build query with filters
        # ====================================================================

        print("\n🔍 Building query...")
        print(f"  Radius: {params.radius_miles} miles")
        if params.sic_codes:
            print(f"  SIC codes: {params.sic_codes}")
        if params.account_categories:
            print(f"  Account categories: {params.account_categories}")
        if params.min_psc_age or params.max_psc_age:
            age_range = f"{params.min_psc_age or '?'} - {params.max_psc_age or '?'}"
            print(f"  PSC age range: {age_range}")

        query = build_query(
            target_lat, target_lon, params
        )

        # ====================================================================
        # STEP 3: Check result count
        # ====================================================================

        print("\n⏳ Executing query...")
        count_query = f"SELECT COUNT(*) FROM ({query})"
        result_count = con.execute(count_query).fetchone()[0]

        print(f"  ✓ Found {result_count:,} matching companies")

        # Debug: Show intermediate counts to understand filter impact
        if params.sic_codes or params.min_psc_age or params.min_company_age_years:
            print("  📊 Filter Analysis:")
            # Debug code disabled - complex query analysis
            active_filters = [f for f in [params.sic_codes, params.min_psc_age,
                                          params.min_company_age_years] if f is not None]
            print(f"    Applied filters: {len(active_filters)} active")

        if result_count == 0:
            print("\n⚠ No companies match the specified criteria")
            print("\nTry:")
            print("  • Increasing the radius")
            print("  • Removing or broadening SIC code filters")
            print("  • Removing or broadening PSC age filters")
            return None

        # Apply max_results limit if specified
        if params.max_results and result_count > params.max_results:
            print(f"  ⚠ Limiting to {params.max_results:,} results (as requested)")
            query += f"\nLIMIT {params.max_results}"
            result_count = params.max_results

        # ====================================================================
        # STEP 4: Export to file
        # ====================================================================

        # Generate output path if not specified
        if params.output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"companies_{timestamp}.{params.output_format}"
            output_path = Path.cwd() / filename
        else:
            output_path = params.output_path

        # Excel row limit check
        if params.output_format == 'xlsx' and result_count > 1_048_576:
            print("\n⚠ Result exceeds Excel row limit (1,048,576)")
            print("  Switching to CSV format")
            output_path = output_path.with_suffix('.csv')
            params.output_format = 'csv'

        print("\n💾 Exporting results...")

        # Export based on format
        if params.output_format == 'csv':
            # Direct CSV export (no pandas)
            con.execute(f"""
                COPY ({query})
                TO '{output_path}'
                (FORMAT CSV, HEADER true)
            """)
            print(f"  ✓ Exported to CSV: {output_path}")

        else:  # xlsx
            # Use chunked pandas approach for memory-efficient Excel export
            # Process data in chunks to avoid loading all into memory at once
            import pandas as pd  # pylint: disable=import-outside-toplevel
            
            # Process in chunks of 10,000 rows
            chunk_size = 10000
            offset = 0
            first_chunk = True
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                while True:
                    chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                    chunk_df = con.execute(chunk_query).df()
                    
                    if chunk_df.empty:
                        break
                    
                    chunk_df.to_excel(writer, index=False, header=first_chunk)
                    first_chunk = False
                    offset += chunk_size
            
            print(f"  ✓ Exported to Excel: {output_path}")

        print("\n" + "=" * 80)
        print("  ✓ Query Complete")
        print("=" * 80)
        print(f"\nOutput: {output_path}")
        print(f"Records: {result_count:,}")
        print("=" * 80)

        return output_path

    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        raise

    finally:
        con.close()

# ============================================================================
# COMMAND LINE INTERFACE (DEPRECATED)
# ============================================================================
# This CLI interface is deprecated. Use find_companies.py instead.



def main():
    """
    DEPRECATED: Use find_companies.py instead

    The CLI interface in query.py has been deprecated in favor of find_companies.py
    which provides better features including config file support and saved profiles.
    """
    print("=" * 80)
    print("  ⚠️  DEPRECATED: query.py CLI is deprecated")
    print("=" * 80)
    print()
    print("Please use find_companies.py instead:")
    print("  python find_companies.py --help")
    print()
    print("Benefits of find_companies.py:")
    print("  • Config file support (config.yaml)")
    print("  • Saved search profiles")
    print("  • Better argument handling")
    print("  • Profile overrides from command line")
    print()
    print("Example usage:")
    print("  python find_companies.py --postcode SW1A1AA --radius 10")
    print("  python find_companies.py --profile it_retirement")
    print()
    print("The query.py module is still available as a library:")
    print("  from src.query import find_companies, QueryParams")
    print()
    print("=" * 80)
    sys.exit(1)

if __name__ == "__main__":
    main()
