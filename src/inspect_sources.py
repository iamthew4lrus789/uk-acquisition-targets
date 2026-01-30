#!/usr/bin/env python3
"""
inspect_sources.py - Validate raw data files structure

Run this after downloading raw data to verify format before running setup.py
"""

import duckdb
from pathlib import Path
import sys
import json

# Configuration - update these paths when new data is downloaded
RAW_DIR = Path("raw")

def find_latest_files():
    """Auto-detect latest data files"""
    import glob

    files = {}

    # Find latest company data file
    company_files = list(RAW_DIR.glob("BasicCompanyDataAsOneFile-*.csv"))
    if company_files:
        files['companies'] = max(company_files)

    # Find latest PSC file
    psc_files = list(RAW_DIR.glob("persons-with-significant-control-snapshot-*.txt"))
    if psc_files:
        files['psc'] = max(psc_files)

    # Find latest ONSPD file
    onspd_files = list(RAW_DIR.glob("ONSPD_*/Data/ONSPD_*_UK.csv"))
    if onspd_files:
        files['postcodes'] = max(onspd_files)

    return files

def inspect_companies(file_path):
    """Inspect BasicCompanyData CSV file"""
    print("\n" + "=" * 80)
    print("  COMPANIES DATA (BasicCompanyDataAsOneFile)")
    print("=" * 80)

    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    print(f"File: {file_path}")
    print(f"Size: {file_path.stat().st_size / (1024**3):.2f} GB")

    con = duckdb.connect()

    try:
        # Sample first few rows
        print("\n📊 Schema Preview:")
        schema = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}', sample_size=10000)").fetchall()
        for col_name, col_type, *_ in schema[:20]:  # Show first 20 columns
            print(f"  {col_name:40} {col_type}")

        if len(schema) > 20:
            print(f"  ... and {len(schema) - 20} more columns")

        # Row count
        print("\n📈 Row Count:")
        count = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{file_path}')").fetchone()[0]
        print(f"  Total companies: {count:,}")

        # Status breakdown
        print("\n📊 Company Status Breakdown:")
        statuses = con.execute(f"""
            SELECT CompanyStatus, COUNT(*) as count
            FROM read_csv_auto('{file_path}')
            GROUP BY CompanyStatus
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        for status, cnt in statuses:
            print(f"  {status:30} {cnt:>10,}")

        # Account categories
        print("\n📊 Account Categories:")
        categories = con.execute(f"""
            SELECT "Accounts.AccountCategory" as category, COUNT(*) as count
            FROM read_csv_auto('{file_path}')
            WHERE "Accounts.AccountCategory" IS NOT NULL
            GROUP BY category
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        for cat, cnt in categories:
            print(f"  {cat:30} {cnt:>10,}")

    except Exception as e:
        print(f"❌ Error inspecting file: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

def inspect_psc(file_path):
    """Inspect PSC JSON file"""
    print("\n" + "=" * 80)
    print("  PSC DATA (Persons with Significant Control)")
    print("=" * 80)

    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    print(f"File: {file_path}")
    print(f"Size: {file_path.stat().st_size / (1024**3):.2f} GB")

    con = duckdb.connect()

    try:
        # Sample first record
        print("\n📊 Sample Record:")
        sample = con.execute(f"""
            SELECT * FROM read_json_auto('{file_path}',
                                        format='newline_delimited',
                                        maximum_object_size=10000000)
            LIMIT 1
        """).fetchone()

        # Show structure (if we can parse it)
        print("  (Sample record structure - abbreviated)")

        # Row count
        print("\n📈 Row Count:")
        count = con.execute(f"""
            SELECT COUNT(*) FROM read_json_auto('{file_path}',
                                                format='newline_delimited',
                                                maximum_object_size=10000000)
        """).fetchone()[0]
        print(f"  Total PSC records: {count:,}")

        # Kind breakdown
        print("\n📊 PSC Kind Breakdown:")
        kinds = con.execute(f"""
            SELECT data.kind, COUNT(*) as count
            FROM read_json_auto('{file_path}',
                               format='newline_delimited',
                               maximum_object_size=10000000)
            GROUP BY data.kind
            ORDER BY count DESC
        """).fetchall()

        for kind, cnt in kinds:
            kind_short = kind.replace('individual-', '').replace('-with-significant-control', '') if kind else 'NULL'
            print(f"  {kind_short:40} {cnt:>10,}")

        # Active vs ceased
        print("\n📊 Active vs Ceased:")
        active_ceased = con.execute(f"""
            SELECT
                CASE WHEN data.ceased_on IS NULL THEN 'Active' ELSE 'Ceased' END as status,
                COUNT(*) as count
            FROM read_json_auto('{file_path}',
                               format='newline_delimited',
                               maximum_object_size=10000000)
            WHERE data.kind = 'individual-person-with-significant-control'
            GROUP BY status
        """).fetchall()

        for status, cnt in active_ceased:
            print(f"  {status:30} {cnt:>10,}")

    except Exception as e:
        print(f"❌ Error inspecting file: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

def inspect_postcodes(file_path):
    """Inspect ONSPD postcode file"""
    print("\n" + "=" * 80)
    print("  POSTCODE DATA (ONS Postcode Directory)")
    print("=" * 80)

    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    print(f"File: {file_path}")
    print(f"Size: {file_path.stat().st_size / (1024**2):.2f} MB")

    con = duckdb.connect()

    try:
        # Schema preview
        print("\n📊 Schema Preview:")
        schema = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}', sample_size=10000)").fetchall()
        for col_name, col_type, *_ in schema[:15]:  # Show first 15 columns
            print(f"  {col_name:40} {col_type}")

        if len(schema) > 15:
            print(f"  ... and {len(schema) - 15} more columns")

        # Row count
        print("\n📈 Row Count:")
        count = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{file_path}')").fetchone()[0]
        print(f"  Total postcode records: {count:,}")

        # Active vs terminated
        print("\n📊 Active vs Terminated:")
        active_term = con.execute(f"""
            SELECT
                CASE WHEN doterm IS NULL THEN 'Active' ELSE 'Terminated' END as status,
                COUNT(*) as count
            FROM read_csv_auto('{file_path}')
            GROUP BY status
        """).fetchall()

        for status, cnt in active_term:
            print(f"  {status:30} {cnt:>10,}")

        # Coordinate bounds check
        print("\n📊 Coordinate Bounds (should be UK):")
        bounds = con.execute(f"""
            SELECT
                MIN(lat) as min_lat, MAX(lat) as max_lat,
                MIN(long) as min_long, MAX(long) as max_long
            FROM read_csv_auto('{file_path}')
            WHERE doterm IS NULL AND lat IS NOT NULL AND long IS NOT NULL
        """).fetchone()

        if bounds:
            min_lat, max_lat, min_long, max_long = bounds
            print(f"  Latitude:  {min_lat:.2f} to {max_lat:.2f}")
            print(f"  Longitude: {min_long:.2f} to {max_long:.2f}")

            # UK should be roughly: lat 49-61, long -8 to 2
            if not (49 <= min_lat and max_lat <= 61):
                print("  ⚠ Warning: Latitude range outside expected UK bounds (49-61)")
            if not (-8 <= min_long and max_long <= 2):
                print("  ⚠ Warning: Longitude range outside expected UK bounds (-8 to 2)")

        # Sample postcodes
        print("\n📊 Sample Postcodes:")
        samples = con.execute(f"""
            SELECT pcds, lat, long
            FROM read_csv_auto('{file_path}')
            WHERE doterm IS NULL AND lat IS NOT NULL
            LIMIT 5
        """).fetchall()

        for pcds, lat, lng in samples:
            print(f"  {pcds:10} ({lat:.6f}, {lng:.6f})")

    except Exception as e:
        print(f"❌ Error inspecting file: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

def main():
    print("=" * 80)
    print("  Companies House Data Inspection Tool")
    print("=" * 80)

    # Find files
    print("\n🔍 Searching for data files in raw/ directory...")
    files = find_latest_files()

    if not files:
        print("\n❌ No data files found in raw/ directory")
        print("\nExpected files:")
        print("  • BasicCompanyDataAsOneFile-YYYY-MM-DD.csv")
        print("  • persons-with-significant-control-snapshot-YYYY-MM-DD.txt")
        print("  • ONSPD_MMM_YYYY/Data/ONSPD_MMM_YYYY_UK.csv")
        print("\nPlease download files from:")
        print("  Companies House: http://download.companieshouse.gov.uk/")
        print("  ONS Postcodes: https://geoportal.statistics.gov.uk/")
        sys.exit(1)

    print(f"\n✓ Found {len(files)} data file(s):")
    for name, path in files.items():
        print(f"  {name:15} {path}")

    # Inspect each file
    if 'companies' in files:
        inspect_companies(files['companies'])

    if 'psc' in files:
        inspect_psc(files['psc'])

    if 'postcodes' in files:
        inspect_postcodes(files['postcodes'])

    # Summary
    print("\n" + "=" * 80)
    print("  ✓ Inspection Complete")
    print("=" * 80)
    print("\nNext steps:")
    print("  1. Review output above for any warnings")
    print("  2. Verify row counts match expectations:")
    print("     • Companies: ~5.7M total, ~5.4M active")
    print("     • PSCs: ~15M total, ~11M active individuals")
    print("     • Postcodes: ~2.7M total, ~1.8M active")
    print("  3. If everything looks good, run: python setup.py")
    print("=" * 80)

if __name__ == "__main__":
    main()
