#!/usr/bin/env python3
"""
setup.py - Prepare Companies House data for querying
Run once after downloading new data files
"""

import duckdb
from pathlib import Path
import sys
from datetime import datetime
import glob

# ============================================================================
# CONFIGURATION
# ============================================================================

RAW_DIR = Path("raw")
PROCESSED_DIR = Path("processed")
PROCESSED_DIR.mkdir(exist_ok=True)

# Configuration - these will be auto-detected, but can be set manually
REQUIRED_FILES = {
    'companies': None,
    'psc': None,
    'postcodes': None
}

# ============================================================================
# FILE DETECTION
# ============================================================================

def find_latest_files():
    """Auto-detect latest data files if multiple versions exist"""

    # Find latest company data file
    company_files = list(RAW_DIR.glob("BasicCompanyDataAsOneFile-*.csv"))
    if company_files:
        REQUIRED_FILES['companies'] = max(company_files)

    # Find latest PSC file
    psc_files = list(RAW_DIR.glob("persons-with-significant-control-snapshot-*.txt"))
    if psc_files:
        REQUIRED_FILES['psc'] = max(psc_files)

    # Find latest ONSPD file
    onspd_files = list(RAW_DIR.glob("ONSPD_*/Data/ONSPD_*_UK.csv"))
    if onspd_files:
        REQUIRED_FILES['postcodes'] = max(onspd_files)

# ============================================================================
# VALIDATION
# ============================================================================

def validate_files():
    """Check all required input files exist"""
    # Try to auto-detect latest files first
    find_latest_files()

    missing = []
    file_sizes = {}

    for name, path in REQUIRED_FILES.items():
        if path is None or not path.exists():
            missing.append(f"{name}: {path if path else 'not detected'}")
        else:
            # Check file size is reasonable
            size_mb = path.stat().st_size / (1024 * 1024)
            file_sizes[name] = size_mb

            # Validate minimum expected sizes
            min_sizes = {'companies': 1000, 'psc': 5000, 'postcodes': 200}  # MB
            if size_mb < min_sizes.get(name, 0):
                print(f"⚠ Warning: {name} file seems small ({size_mb:.1f}MB)")

    if missing:
        print("❌ Missing required files:")
        for item in missing:
            print(f"  {item}")
        print("\nExpected files in raw/ directory:")
        print("  • BasicCompanyDataAsOneFile-YYYY-MM-DD.csv")
        print("  • persons-with-significant-control-snapshot-YYYY-MM-DD.txt")
        print("  • ONSPD_MMM_YYYY/Data/ONSPD_MMM_YYYY_UK.csv")
        print("\nDownload from:")
        print("  Companies: http://download.companieshouse.gov.uk/")
        print("  Postcodes: https://geoportal.statistics.gov.uk/")
        sys.exit(1)

    print("✓ All required files present")
    for name, size in file_sizes.items():
        print(f"  {name:15} {size:>10.1f} MB")

# ============================================================================
# COMPANIES DATA
# ============================================================================

def setup_companies(con):
    """
    Convert BasicCompanyData to Parquet

    Excludes:
    - SIC codes (normalized separately)
    - Previous names (not needed)
    - Detailed address fields (PostCode sufficient)
    - Mortgage/partnership fields (rarely used)
    """
    print("\n📊 Processing companies...")

    con.execute(f"""
        COPY (
            SELECT
                CompanyNumber,
                CompanyName,
                "RegAddress.PostCode" AS RegAddressPostCode,
                CompanyStatus,
                CompanyCategory,
                CountryOfOrigin,
                IncorporationDate,
                "Accounts.AccountCategory" AS AccountCategory,
                "Accounts.NextDueDate" AS AccountsNextDueDate,
                "Accounts.LastMadeUpDate" AS AccountsLastMadeUpDate,
                ConfStmtNextDueDate,
                ConfStmtLastMadeUpDate
            FROM read_csv_auto('{REQUIRED_FILES["companies"]}',
                               header=true,
                               ignore_errors=false)
            WHERE CompanyNumber IS NOT NULL
        ) TO '{PROCESSED_DIR}/companies.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    # Verify and report
    count = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/companies.parquet'"
    ).fetchone()[0]

    active_count = con.execute(f"""
        SELECT COUNT(*) FROM '{PROCESSED_DIR}/companies.parquet'
        WHERE CompanyStatus = 'Active'
    """).fetchone()[0]

    print(f"  ✓ Processed {count:,} companies ({active_count:,} active)")

    # Check for issues
    null_postcodes = con.execute(f"""
        SELECT COUNT(*) FROM '{PROCESSED_DIR}/companies.parquet'
        WHERE CompanyStatus = 'Active' AND RegAddressPostCode IS NULL
    """).fetchone()[0]

    if null_postcodes > 0:
        print(f"  ⚠ Warning: {null_postcodes:,} active companies have no postcode")

# ============================================================================
# SIC CODES (NORMALIZED)
# ============================================================================

def setup_company_sic(con):
    """
    Normalize SIC codes from SicText_1..4 columns into rows

    Input format: "62012 - Business and domestic software development"
    Output: sic_code=62012, sic_description="Business and domestic..."
    """
    print("\n🏢 Normalizing SIC codes...")

    con.execute(f"""
        COPY (
            SELECT
                CompanyNumber,
                sic_code,
                sic_description,
                sic_position
            FROM (
                -- SicText_1
                SELECT
                    CompanyNumber,
                    TRY_CAST(SPLIT_PART("SICCode.SicText_1", ' - ', 1) AS INTEGER) AS sic_code,
                    SPLIT_PART("SICCode.SicText_1", ' - ', 2) AS sic_description,
                    1 AS sic_position
                FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
                WHERE "SICCode.SicText_1" IS NOT NULL

                UNION ALL

                -- SicText_2
                SELECT
                    CompanyNumber,
                    TRY_CAST(SPLIT_PART("SICCode.SicText_2", ' - ', 1) AS INTEGER),
                    SPLIT_PART("SICCode.SicText_2", ' - ', 2),
                    2
                FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
                WHERE "SICCode.SicText_2" IS NOT NULL

                UNION ALL

                -- SicText_3
                SELECT
                    CompanyNumber,
                    TRY_CAST(SPLIT_PART("SICCode.SicText_3", ' - ', 1) AS INTEGER),
                    SPLIT_PART("SICCode.SicText_3", ' - ', 2),
                    3
                FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
                WHERE "SICCode.SicText_3" IS NOT NULL

                UNION ALL

                -- SicText_4
                SELECT
                    CompanyNumber,
                    TRY_CAST(SPLIT_PART("SICCode.SicText_4", ' - ', 1) AS INTEGER),
                    SPLIT_PART("SICCode.SicText_4", ' - ', 2),
                    4
                FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
                WHERE "SICCode.SicText_4" IS NOT NULL
            )
            WHERE sic_code IS NOT NULL  -- Filter failed CAST attempts
        ) TO '{PROCESSED_DIR}/company_sic.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Report statistics
    count = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/company_sic.parquet'"
    ).fetchone()[0]

    unique_codes = con.execute(
        f"SELECT COUNT(DISTINCT sic_code) FROM '{PROCESSED_DIR}/company_sic.parquet'"
    ).fetchone()[0]

    unique_companies = con.execute(
        f"SELECT COUNT(DISTINCT CompanyNumber) FROM '{PROCESSED_DIR}/company_sic.parquet'"
    ).fetchone()[0]

    print(f"  ✓ Extracted {count:,} company-SIC mappings")
    print(f"    {unique_codes:,} unique SIC codes")
    print(f"    {unique_companies:,} companies with SIC codes")


def report_sic_parse_errors(con):
    """Report how many SIC codes failed to parse (data quality visibility)"""

    # Count total SIC fields across all companies
    total_sic_fields = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE "SICCode.SicText_1" IS NOT NULL) +
            COUNT(*) FILTER (WHERE "SICCode.SicText_2" IS NOT NULL) +
            COUNT(*) FILTER (WHERE "SICCode.SicText_3" IS NOT NULL) +
            COUNT(*) FILTER (WHERE "SICCode.SicText_4" IS NOT NULL)
            AS total_fields
        FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
    """).fetchone()[0]

    # Count successfully parsed codes
    parsed_codes = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/company_sic.parquet'"
    ).fetchone()[0]

    failed = total_sic_fields - parsed_codes
    if failed > 0:
        pct = (failed / total_sic_fields) * 100
        print(f"  ⚠ Failed to parse {failed:,} SIC codes ({pct:.1f}%)")

        # Show sample of failed codes
        samples = con.execute(f"""
            SELECT "SICCode.SicText_1"
            FROM read_csv_auto('{REQUIRED_FILES["companies"]}')
            WHERE "SICCode.SicText_1" IS NOT NULL
              AND TRY_CAST(SPLIT_PART("SICCode.SicText_1", ' - ', 1) AS INTEGER) IS NULL
            LIMIT 5
        """).fetchall()

        if samples:
            print("  Sample failed SIC codes:")
            for (code,) in samples:
                print(f"    '{code}'")

# ============================================================================
# POSTCODES
# ============================================================================

def setup_postcodes(con):
    """
    Extract minimal ONSPD fields for geospatial queries

    Filters:
    - doterm IS NULL (exclude terminated postcodes)
    - lat/long NOT NULL (exclude incomplete records)
    """
    print("\n📍 Processing postcodes...")

    con.execute(f"""
        COPY (
            SELECT
                pcds,           -- Formatted postcode: "SW1A 1AA"
                lat,            -- Latitude (WGS84)
                long,           -- Longitude (WGS84)
                lsoa21          -- Statistical area (optional, for future use)
            FROM read_csv_auto('{REQUIRED_FILES["postcodes"]}',
                               header=true)
            WHERE lat IS NOT NULL
              AND long IS NOT NULL
              AND doterm IS NULL  -- Exclude terminated postcodes
        ) TO '{PROCESSED_DIR}/postcodes.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Report statistics
    count = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/postcodes.parquet'"
    ).fetchone()[0]

    # Sanity check coordinates (UK bounds)
    coord_check = con.execute(f"""
        SELECT
            MIN(lat) as min_lat, MAX(lat) as max_lat,
            MIN(long) as min_long, MAX(long) as max_long
        FROM '{PROCESSED_DIR}/postcodes.parquet'
    """).fetchone()

    print(f"  ✓ Processed {count:,} active postcodes")
    print(f"    Latitude range: {coord_check[0]:.2f} to {coord_check[1]:.2f}")
    print(f"    Longitude range: {coord_check[2]:.2f} to {coord_check[3]:.2f}")

    # UK should be roughly: lat 49-61, long -8 to 2
    if not (49 <= coord_check[0] and coord_check[1] <= 61):
        print(f"  ⚠ Warning: Latitude range outside expected UK bounds")
    if not (-8 <= coord_check[2] and coord_check[3] <= 2):
        print(f"  ⚠ Warning: Longitude range outside expected UK bounds")

# ============================================================================
# PSC DATA
# ============================================================================

def setup_psc(con):
    """
    Extract PSC data with calculated approximate age

    Filters:
    - kind = 'individual-person-with-significant-control' (not corporate)
    - ceased_on IS NULL (active PSCs only)

    Age calculation: year(current_date) - birth_year
    Accuracy: ±12 months (only year available)
    """
    print("\n👤 Processing PSC data...")

    con.execute(f"""
        COPY (
            SELECT
                company_number,
                data.name,
                data.kind,
                data.date_of_birth.year AS birth_year,
                YEAR(CURRENT_DATE) - data.date_of_birth.year AS approximate_age,
                data.nationality,
                data.natures_of_control,
                data.notified_on
            FROM read_json_auto('{REQUIRED_FILES["psc"]}',
                                format='newline_delimited',
                                maximum_object_size=10000000,
                                union_by_name=true,
                                ignore_errors=true)
            WHERE data.kind = 'individual-person-with-significant-control'
              AND data.ceased_on IS NULL  -- Active PSCs only
              AND data.date_of_birth.year IS NOT NULL
        ) TO '{PROCESSED_DIR}/psc.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Report statistics
    count = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/psc.parquet'"
    ).fetchone()[0]

    unique_companies = con.execute(
        f"SELECT COUNT(DISTINCT company_number) FROM '{PROCESSED_DIR}/psc.parquet'"
    ).fetchone()[0]

    age_stats = con.execute(f"""
        SELECT
            MIN(approximate_age) as min_age,
            AVG(approximate_age) as avg_age,
            MAX(approximate_age) as max_age
        FROM '{PROCESSED_DIR}/psc.parquet'
    """).fetchone()

    print(f"  ✓ Processed {count:,} active PSCs")
    print(f"    {unique_companies:,} companies with PSCs")
    print(f"    Age range: {age_stats[0]} - {age_stats[2]} (avg: {age_stats[1]:.1f})")
    print(f"  ⚠ Note: Age accuracy ±12 months (birth year only)")


def count_psc_lines(file_path):
    """Count lines in PSC file for error reporting"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)


def report_psc_parse_errors(con, file_path):
    """Report PSC parsing issues (data quality visibility)"""

    total_lines = count_psc_lines(file_path)
    parsed_records = con.execute(
        f"SELECT COUNT(*) FROM '{PROCESSED_DIR}/psc.parquet'"
    ).fetchone()[0]

    dropped = total_lines - parsed_records
    if dropped > 0:
        pct = (dropped / total_lines) * 100
        print(f"  ⚠ Dropped {dropped:,} PSC records during parsing ({pct:.1f}%)")
        print(f"    This may be due to:")
        print(f"      • Non-individual PSCs (corporate entities)")
        print(f"      • Ceased PSC records")
        print(f"      • Malformed JSON")
        print(f"      • Missing birth year data")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 80)
    print("  Companies House Data Pipeline - Setup")
    print("=" * 80)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Validation
    validate_files()

    # Initialize DuckDB (in-memory, no persistence needed)
    con = duckdb.connect()

    try:
        # Process each dataset
        setup_companies(con)
        setup_company_sic(con)
        report_sic_parse_errors(con)  # Report data quality issues
        setup_postcodes(con)
        setup_psc(con)
        report_psc_parse_errors(con, REQUIRED_FILES["psc"])  # Report data quality issues

        # Summary
        print("\n" + "=" * 80)
        print("  ✓ Setup Complete!")
        print("=" * 80)
        print(f"\nProcessed files location: {PROCESSED_DIR.absolute()}")
        print("\nGenerated files:")
        for file in sorted(PROCESSED_DIR.glob("*.parquet")):
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"  {file.name:30} {size_mb:>8.1f} MB")

        print("\nNext steps:")
        print("  1. Review output above for warnings")
        print("  2. Run queries with: python query.py")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        con.close()

if __name__ == "__main__":
    main()
