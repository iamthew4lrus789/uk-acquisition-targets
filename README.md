# UK Acquisition Targets

A CLI tool for identifying UK company acquisition targets using publicly available Companies House data. Query 5.7M+ company records with geographic, industry, ownership, and demographic filters.

## Features

- **Geographic Search**: Find companies within N miles of any UK postcode using Haversine distance
- **Industry Filtering**: Filter by Standard Industrial Classification (SIC) codes
- **Owner Demographics**: Filter by age of Persons with Significant Control (PSC)
- **Tenure Analysis**: Filter by how long owners have held their positions
- **Company Age**: Filter by years since incorporation
- **Company Size**: Filter by account categories (MICRO, SMALL, MEDIUM, etc.)
- **Fast Queries**: 5-30 second response times on 5.7M+ records
- **Flexible Export**: Results in CSV or Excel format
- **Saved Profiles**: Save common search configurations in YAML

## Quick Start

### 1. Prerequisites

- Python 3.10+
- ~15GB disk space for data files
- 2-4GB RAM for queries

### 2. Installation

```bash
git clone https://github.com/yourusername/uk-acquisition-targets.git
cd uk-acquisition-targets

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Download Data

You need four datasets. For a comprehensive guide to Companies House bulk data, see [chguide.co.uk](https://chguide.co.uk/).

#### BasicCompanyData (Free Download)

Download from: http://download.companieshouse.gov.uk/en_output.html

Look for: `BasicCompanyDataAsOneFile-YYYY-MM-DD.csv` (~2GB)

Place in: `raw/`

#### PSC Snapshot (Free Download)

Download from: http://download.companieshouse.gov.uk/en_pscdata.html

Look for: `persons-with-significant-control-snapshot-YYYY-MM-DD.txt` (~8GB)

Place in: `raw/`

#### Officers Bulk Data (Request Required)

**Important**: The PSC register only started in 2016/2017, so PSC tenure data is capped at ~8 years. For accurate tenure analysis (e.g., "director since 2001"), you need the Officers bulk dataset.

To request: Email `BulkProducts@companieshouse.gov.uk`

For more information: [Companies House Forum Discussion](https://forum.companieshouse.gov.uk/t/can-we-bulk-download-officer-director-data/1225/395)

Place in: `raw/`

#### ONS Postcode Directory (Free Download)

Download from: https://www.ons.gov.uk/methodology/geography/geographicalproducts/postcodeproducts

Look for: ONSPD (ONS Postcode Directory) - latest quarterly release (~400MB ZIP)

Extract to: `raw/ONSPD_MMM_YYYY/`

#### Expected Directory Structure

After downloading, your `raw/` directory should look like:

```
raw/
├── BasicCompanyDataAsOneFile-2025-01-01.csv
├── persons-with-significant-control-snapshot-2025-01-01.txt
├── officers-bulk-YYYY-MM-DD.csv  (if obtained)
└── ONSPD_FEB_2025/
    └── Data/
        └── ONSPD_FEB_2025_UK.csv
```

### 4. Process Data

Convert raw CSV files to optimized Parquet format:

```bash
python src/setup.py
```

**Duration**: 10-15 minutes

This creates ~1.5GB of Parquet files in `processed/`.

### 5. Run Queries

```bash
# Using the wrapper script
./run_find_companies.sh --postcode "SW1A 1AA" --radius 10

# Or directly with Python
PYTHONPATH=. python src/find_companies.py --postcode "SW1A 1AA" --radius 10

# Using a saved profile
./run_find_companies.sh --profile it_retirement

# List available profiles
./run_find_companies.sh --list-profiles
```

## Usage Examples

### Basic Geographic Search

Find all active companies within 10 miles of Westminster:

```bash
./run_find_companies.sh --postcode "SW1A 1AA" --radius 10
```

### Targeted Acquisition Search

HVAC businesses with retirement-age owners:

```bash
./run_find_companies.sh \
  --postcode "EC2R 8AH" \
  --radius 15 \
  --sic 43220 43290 \
  --min-psc-age 55 \
  --max-psc-age 75 \
  --min-company-age 5 \
  --categories "MICRO ENTITY" "SMALL" \
  --format xlsx
```

### IT Consultancies

```bash
./run_find_companies.sh \
  --postcode "EC1A 1BB" \
  --radius 25 \
  --sic 62020 62012 62090 \
  --min-psc-age 50 \
  --format csv
```

## Query Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `--postcode, -p` | str | UK postcode (required) | `"SW1A 1AA"` |
| `--radius, -r` | float | Search radius in miles (required, max 500) | `15` |
| `--sic, -s` | int[] | SIC codes (5 digits) | `62020 62090` |
| `--categories, -c` | str[] | Account categories | `"MICRO ENTITY" "SMALL"` |
| `--min-psc-age` | int | Minimum PSC age (16-120) | `50` |
| `--max-psc-age` | int | Maximum PSC age (16-120) | `70` |
| `--min-psc-tenure` | int | Minimum years as PSC/officer | `5` |
| `--max-psc-tenure` | int | Maximum years as PSC/officer | `20` |
| `--min-company-age` | int | Minimum years since incorporation | `5` |
| `--max-company-age` | int | Maximum years since incorporation | `30` |
| `--format, -f` | str | Output format: `csv` or `xlsx` | `xlsx` |
| `--output, -o` | path | Custom output file path | `results.csv` |
| `--profile` | str | Use saved profile from config.yaml | `it_retirement` |

## Configuration Profiles

Save common searches in `config.yaml`:

```yaml
profiles:
  it_retirement:
    description: "IT consultancies with retirement-age owners"
    postcode: "SW1A 1AA"
    radius_miles: 15
    sic_codes:
      - 62020  # IT consultancy
      - 62090  # Other IT services
    min_company_age_years: 5
    min_psc_age: 60
    output_format: csv
```

Use with: `./run_find_companies.sh --profile it_retirement`

## Common SIC Codes

| Code | Industry |
|------|----------|
| 43220 | Plumbing, heat and air-conditioning installation |
| 43290 | Other construction installation |
| 62012 | Business and domestic software development |
| 62020 | Information technology consultancy |
| 62090 | Other information technology services |
| 71121 | Engineering design activities |
| 71122 | Engineering related consultancy |
| 73110 | Advertising agencies |
| 73120 | Media representation services |
| 69201 | Accounting and auditing activities |

Full list: [UK SIC Codes](https://www.gov.uk/government/publications/standard-industrial-classification-of-economic-activities-sic)

## Account Categories

| Category | Description |
|----------|-------------|
| `MICRO ENTITY` | Turnover ≤ £632k (~1.6M companies) |
| `SMALL` | Turnover ≤ £10.2M (~63k companies) |
| `MEDIUM` | Turnover ≤ £36M (~5k companies) |
| `FULL` | Large companies (~80k companies) |
| `DORMANT` | No trading activity (~603k companies) |

Use `--list-categories` to see all valid categories.

## Known Limitations

1. **PSC Age Accuracy**: ±12 months (only birth year available in source data)
2. **PSC Tenure**: Capped at ~8 years without Officers data (PSC register started 2016/2017)
3. **Postcode Matching**: ~5% may not match due to format variations
4. **Data Freshness**: Companies House releases monthly snapshots
5. **No Financial Metrics**: Use AccountCategory for initial sizing only

## Troubleshooting

### "Postcode not found"

- Verify postcode on [Royal Mail Postcode Finder](https://www.royalmail.com/find-a-postcode)
- Try nearby postcode
- Check ONSPD is latest version

### "Missing processed files"

Run the data processing step:

```bash
python src/setup.py
```

### Query returns zero results

- Increase `radius_miles`
- Remove or broaden `sic_codes` filter
- Widen PSC age range
- Remove `account_categories` filter

### Query takes > 30 seconds

- Reduce `radius_miles`
- Add more filters to narrow results
- Use CSV instead of XLSX for large result sets

## Data Sources

- [Companies House Free Data](http://download.companieshouse.gov.uk/) - Company records, PSC data
- [ONS Postcode Directory](https://www.ons.gov.uk/methodology/geography/geographicalproducts/postcodeproducts) - UK postcode coordinates
- [Companies House Bulk Data Guide](https://chguide.co.uk/) - Comprehensive guide to available datasets

## License

MIT License - see [LICENSE](LICENSE) file.

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Technical Details

- **Database**: DuckDB (embedded analytical database)
- **Storage**: Apache Parquet with ZSTD compression
- **Query Performance**: 5-30 seconds typical
- **Data Coverage**: ~5.7M companies, ~15M PSCs, ~2.7M postcodes
