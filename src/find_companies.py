#!/usr/bin/env python3
"""
find_companies.py - Command-line interface for querying Companies House data

Usage examples:
  # Basic geographic search
  python find_companies.py --postcode "SW1A 1AA" --radius 10

  # With industry filter
  python find_companies.py --postcode "SW1A 1AA" --radius 10 --sic 62020 62090

  # With all filters
  python find_companies.py --postcode "SW1A 1AA" --radius 10 \
    --sic 62020 62090 \
    --min-company-age 5 \
    --min-psc-age 60 \
    --min-psc-tenure 2 \
    --categories "MICRO ENTITY" "SMALL" \
    --format xlsx
"""

import argparse
import sys
from pathlib import Path
import yaml
from datetime import datetime

# Import from query module
from src.query import find_companies, QueryParams


def load_config(config_path='config.yaml'):
    """Load configuration file"""
    config_file = Path(config_path)
    if not config_file.exists():
        return {'defaults': {}, 'profiles': {}}

    try:
        with open(config_file, encoding='utf-8') as f:
            return yaml.safe_load(f) or {'defaults': {}, 'profiles': {}}
    except Exception as e:
        print(f"⚠ Warning: Failed to load config file: {e}")
        return {'defaults': {}, 'profiles': {}}


def create_parser():
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(
        description='Query Companies House data for acquisition target identification',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using a saved profile
  %(prog)s --profile it_retirement

  # Override profile settings
  %(prog)s --profile it_retirement --radius 15

  # Direct command-line (no config)
  %(prog)s --postcode "SW1A 1AA" --radius 10 --sic 62020 62090 \\
    --min-company-age 5 --min-psc-age 60

  # List available profiles
  %(prog)s --list-profiles

Common SIC Codes:
  43220 - Plumbing, HVAC installation
  62012 - Software development
  62020 - IT consultancy
  62090 - Other IT services
  71121 - Engineering design
  73110 - Advertising agencies
        """
    )

    # Config options
    config = parser.add_argument_group('configuration')
    config.add_argument(
        '--config',
        default='config.yaml',
        help='Config file path (default: config.yaml)'
    )
    config.add_argument(
        '--profile',
        help='Use named profile from config file'
    )
    config.add_argument(
        '--list-profiles',
        action='store_true',
        help='List available profiles and exit'
    )

    # Required arguments (optional if using profile)
    required = parser.add_argument_group('required arguments (or use --profile)')
    required.add_argument(
        '--postcode', '-p',
        help='UK postcode (e.g., "SW1A 1AA", "EC2R 8AH")'
    )
    required.add_argument(
        '--radius', '-r',
        type=float,
        help='Search radius in miles (max 500)'
    )

    # Industry filters
    industry = parser.add_argument_group('industry filters')
    industry.add_argument(
        '--sic', '-s',
        type=int,
        nargs='+',
        metavar='CODE',
        help='SIC codes to filter (5-digit codes, e.g., 62020 62090)'
    )

    # Company filters
    company = parser.add_argument_group('company filters')
    company.add_argument(
        '--categories', '-c',
        nargs='+',
        metavar='CATEGORY',
        help='Account categories (e.g., "MICRO ENTITY" "SMALL" "MEDIUM")'
    )
    company.add_argument(
        '--status',
        default='Active',
        help='Company status (default: Active)'
    )
    company.add_argument(
        '--min-company-age',
        type=int,
        metavar='YEARS',
        help='Minimum company age in years (e.g., 5, 10)'
    )
    company.add_argument(
        '--max-company-age',
        type=int,
        metavar='YEARS',
        help='Maximum company age in years'
    )

    # PSC (owner) filters
    psc = parser.add_argument_group('owner (PSC) filters')
    psc.add_argument(
        '--min-psc-age',
        type=int,
        metavar='AGE',
        help='Minimum PSC age (e.g., 50, 60)'
    )
    psc.add_argument(
        '--max-psc-age',
        type=int,
        metavar='AGE',
        help='Maximum PSC age (e.g., 70, 75)'
    )
    psc.add_argument(
        '--min-psc-tenure',
        type=int,
        metavar='YEARS',
        help='Minimum PSC tenure in years (e.g., 2, 5)'
    )
    psc.add_argument(
        '--max-psc-tenure',
        type=int,
        metavar='YEARS',
        help='Maximum PSC tenure in years (e.g., 10, 15)'
    )
    psc.add_argument(
        '--strict-psc-tenure',
        action='store_true',
        help='Strict PSC tenure mode - ALL PSCs must meet tenure criteria'
    )

    # Output options
    output = parser.add_argument_group('output options')
    output.add_argument(
        '--format', '-f',
        choices=['csv', 'xlsx'],
        default='csv',
        help='Output format (default: csv)'
    )
    output.add_argument(
        '--output', '-o',
        type=Path,
        metavar='FILE',
        help='Output file path (default: auto-generated timestamp)'
    )
    output.add_argument(
        '--max-results',
        type=int,
        metavar='N',
        help='Limit number of results'
    )

    # Informational
    parser.add_argument(
        '--list-categories',
        action='store_true',
        help='List all valid account categories and exit'
    )

    return parser


def list_account_categories():
    """Display valid account categories"""
    categories = [
        ('MICRO ENTITY', 'Turnover ≤ £632k, ~1.6M companies'),
        ('SMALL', 'Turnover ≤ £10.2M, ~63k companies'),
        ('MEDIUM', 'Turnover ≤ £36M, ~5k companies'),
        ('FULL', 'Large companies, ~80k companies'),
        ('TOTAL EXEMPTION FULL', 'Small companies filing full accounts, ~1.3M'),
        ('TOTAL EXEMPTION SMALL', 'Small companies, ~9k'),
        ('DORMANT', 'No trading activity, ~603k companies'),
        ('NO ACCOUNTS FILED', '~1.5M companies'),
        ('UNAUDITED ABRIDGED', '~165k companies'),
        ('AUDITED ABRIDGED', 'Smaller set'),
        ('AUDIT EXEMPTION SUBSIDIARY', '~31k companies'),
        ('FILING EXEMPTION SUBSIDIARY', 'Various'),
        ('GROUP', '~28k companies'),
        ('PARTIAL EXEMPTION', 'Various'),
    ]

    print("Valid Account Categories:")
    print("=" * 80)
    for cat, desc in categories:
        print(f"  {cat:30} - {desc}")
    print("\nUsage: --categories \"MICRO ENTITY\" \"SMALL\"")


def create_command_file(args, output_path):
    """
    Create a companion text file with the CLI command used to generate the output

    Args:
        args: Parsed command-line arguments
        output_path: Path to the output file (CSV/XLSX)
    
    Returns:
        Path to the created text file
    """
    # Create text file path (same name, .txt extension)
    txt_path = output_path.with_suffix('.txt')

    # Reconstruct the CLI command
    command_parts = [sys.argv[0]]

    # Add all arguments that were actually used
    if hasattr(args, 'config') and args.config != 'config.yaml':
        command_parts.extend(['--config', args.config])

    if hasattr(args, 'profile') and args.profile:
        command_parts.extend(['--profile', args.profile])

    if hasattr(args, 'postcode') and args.postcode:
        command_parts.extend(['--postcode', args.postcode])

    if hasattr(args, 'radius') and args.radius:
        command_parts.extend(['--radius', str(args.radius)])

    if hasattr(args, 'sic') and args.sic:
        command_parts.extend(['--sic'] + [str(s) for s in args.sic])

    if hasattr(args, 'categories') and args.categories:
        command_parts.extend(['--categories'] + args.categories)

    if hasattr(args, 'status') and args.status != 'Active':
        command_parts.extend(['--status', args.status])

    if hasattr(args, 'min_psc_age') and args.min_psc_age:
        command_parts.extend(['--min-psc-age', str(args.min_psc_age)])

    if hasattr(args, 'max_psc_age') and args.max_psc_age:
        command_parts.extend(['--max-psc-age', str(args.max_psc_age)])

    if hasattr(args, 'min_psc_tenure') and args.min_psc_tenure:
        command_parts.extend(['--min-psc-tenure', str(args.min_psc_tenure)])

    if hasattr(args, 'max_psc_tenure') and args.max_psc_tenure:
        command_parts.extend(['--max-psc-tenure', str(args.max_psc_tenure)])

    if hasattr(args, 'strict_psc_tenure') and args.strict_psc_tenure:
        command_parts.append('--strict-psc-tenure')

    if hasattr(args, 'min_company_age') and args.min_company_age:
        command_parts.extend(['--min-company-age', str(args.min_company_age)])

    if hasattr(args, 'max_company_age') and args.max_company_age:
        command_parts.extend(['--max-company-age', str(args.max_company_age)])

    if hasattr(args, 'format') and args.format != 'csv':
        command_parts.extend(['--format', args.format])

    if hasattr(args, 'output') and args.output:
        command_parts.extend(['--output', str(args.output)])

    if hasattr(args, 'max_results') and args.max_results:
        command_parts.extend(['--max-results', str(args.max_results)])

    # Write command to text file
    command_str = ' '.join(command_parts)

    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("# Companies House Query Command\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Results file: {output_path.name}\n")
        f.write("# Command:\n")
        f.write(command_str + "\n")

    return txt_path


def main():  # pylint: disable=too-many-branches,too-many-statements
    """Main entry point for find_companies CLI"""
    parser = create_parser()

    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # Handle --list-categories
    if args.list_categories:
        list_account_categories()
        sys.exit(0)

    # Load config file
    config = load_config(args.config)

    # Handle --list-profiles
    if args.list_profiles:
        profiles = config.get('profiles', {})
        if not profiles:
            print("No profiles defined in config file.")
            sys.exit(0)

        print("Available profiles:")
        print("=" * 80)
        for name, settings in profiles.items():
            desc = settings.get('description', 'No description')
            print(f"\n  {name}")
            print(f"    {desc}")

            # Show key settings
            if 'postcode' in settings:
                print(f"    Postcode: {settings['postcode']}")
            if 'radius_miles' in settings:
                print(f"    Radius: {settings['radius_miles']} miles")
            if 'sic_codes' in settings:
                print(f"    SIC codes: {settings['sic_codes']}")
            if 'min_psc_age' in settings or 'max_psc_age' in settings:
                age_str = f"{settings.get('min_psc_age', '?')}-{settings.get('max_psc_age', '?')}"
                print(f"    PSC age: {age_str}")

        print("\n" + "=" * 80)
        print("\nUsage: python find_companies.py --profile PROFILE_NAME")
        sys.exit(0)

    # Build parameter dict by merging: defaults → profile → command-line args
    params_dict = {}

    # Step 1: Apply defaults
    defaults = config.get('defaults', {})
    params_dict.update(defaults)

    # Step 2: Apply profile settings (if specified)
    if args.profile:
        profiles = config.get('profiles', {})
        if args.profile not in profiles:
            print(f"\n❌ Profile '{args.profile}' not found in config file")
            print(f"\nAvailable profiles: {', '.join(profiles.keys())}")
            print("\nUse --list-profiles to see details")
            sys.exit(1)

        profile = profiles[args.profile]
        params_dict.update(profile)
        # Remove description field (not a QueryParams field)
        params_dict.pop('description', None)

    # Step 3: Apply command-line arguments (override config/profile)
    # Only override if explicitly provided on command line
    if args.postcode is not None:
        params_dict['postcode'] = args.postcode
    if args.radius is not None:
        params_dict['radius_miles'] = args.radius
    if args.sic is not None:
        params_dict['sic_codes'] = args.sic
    if args.categories is not None:
        params_dict['account_categories'] = args.categories
    if args.status != 'Active':  # Only override if not default
        params_dict['company_status'] = args.status
    if args.min_psc_age is not None:
        params_dict['min_psc_age'] = args.min_psc_age
    if args.max_psc_age is not None:
        params_dict['max_psc_age'] = args.max_psc_age
    if args.min_psc_tenure is not None:
        params_dict['min_psc_tenure_years'] = args.min_psc_tenure
    if args.max_psc_tenure is not None:
        params_dict['max_psc_tenure_years'] = args.max_psc_tenure
    if args.strict_psc_tenure:
        params_dict['strict_psc_tenure'] = args.strict_psc_tenure
    if args.min_company_age is not None:
        params_dict['min_company_age_years'] = args.min_company_age
    if args.max_company_age is not None:
        params_dict['max_company_age_years'] = args.max_company_age
    if args.format != 'csv':  # Only override if not default
        params_dict['output_format'] = args.format
    if args.output is not None:
        params_dict['output_path'] = args.output
    if args.max_results is not None:
        params_dict['max_results'] = args.max_results

    # Validate required parameters
    if 'postcode' not in params_dict or 'radius_miles' not in params_dict:
        print("\n❌ Missing required parameters: postcode and radius")
        print("\nEither:")
        print("  • Use a profile: --profile PROFILE_NAME")
        print("  • Provide directly: --postcode POSTCODE --radius MILES")
        print("\nUse --list-profiles to see available profiles")
        sys.exit(1)

    # Build QueryParams from merged settings
    # Note: postcode and radius_miles are now REQUIRED (not Optional)
    try:
        params = QueryParams(
            postcode=params_dict['postcode'],  # Required - will raise KeyError if missing
            radius_miles=params_dict['radius_miles'],  # Required - will raise KeyError if missing
            sic_codes=params_dict.get('sic_codes'),
            account_categories=params_dict.get('account_categories'),
            company_status=params_dict.get('company_status', 'Active'),
            min_psc_age=params_dict.get('min_psc_age'),
            max_psc_age=params_dict.get('max_psc_age'),
            min_psc_tenure_years=params_dict.get('min_psc_tenure_years'),
            max_psc_tenure_years=params_dict.get('max_psc_tenure_years'),
            strict_psc_tenure=params_dict.get('strict_psc_tenure'),
            min_company_age_years=params_dict.get('min_company_age_years'),
            max_company_age_years=params_dict.get('max_company_age_years'),
            output_format=params_dict.get('output_format', 'csv'),
            output_path=params_dict.get('output_path'),
            max_results=params_dict.get('max_results')
        )

        # Execute query
        output_file = find_companies(params)

        if output_file:
            # Create companion text file with CLI command
            command_file = create_command_file(args, output_file)

            print(f"\n✓ Success! Results saved to: {output_file}")
            print(f"✓ Command log saved to: {command_file}")
            print("\nTo view results:")
            if params.output_format == 'csv':
                print(f"  head {output_file}")
                print(f"  cat {output_file} | column -t -s,")
            else:
                print(f"  libreoffice {output_file}")
            print(f"\nTo recreate this query:")
            print(f"  {command_file.read_text().split('# Command:')[1].strip()}")
            sys.exit(0)
        else:
            print("\n⚠ No results found. Try broadening your search criteria.")
            sys.exit(1)

    except ValueError as e:
        print(f"\n❌ Invalid parameters: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        print("\nRun 'python setup.py' first to process the data.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        import traceback  # pylint: disable=import-outside-toplevel
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
