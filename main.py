"""
Azure AI Foundry Cost Pipeline - Main Orchestrator

This is the main entry point for the cost extraction pipeline.
It orchestrates the flow:
    1. Load configuration
    2. Authenticate with Azure
    3. Extract cost data from all configured scopes
    4. Write to CSV (local testing) or Delta Lake (production)

Supports two storage modes:
    - CSV: For local testing (no Spark/Databricks required)
    - Delta: For production (requires Databricks Runtime)

Usage:
    - As script: python main.py [--date YYYY-MM-DD] [--days N] [--no-merge]
    - As Databricks notebook: Just run the cells
    
Example:
    # Run for yesterday (default)
    python main.py
    
    # Run for specific date
    python main.py --date 2026-01-05
    
    # Backfill last 7 days
    python main.py --days 7
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("CostPipeline")


def run_pipeline(
    target_dates: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_merge: bool = True
) -> dict:
    """
    Main pipeline execution function.
    
    Args:
        target_dates: List of dates to process (YYYY-MM-DD format).
                     Defaults to yesterday if not provided.
        start_date: Start date for date range extraction (YYYY-MM-DD)
        end_date: End date for date range extraction (YYYY-MM-DD)
        use_merge: If True, use MERGE for idempotency. If False, use APPEND.
        
    Returns:
        dict: Pipeline execution summary
    """
    # Import modules here to allow for cleaner error messages if dependencies missing
    from config import load_config
    from auth import AzureAuthenticator
    from data_extractor import AzureCostExtractor
    
    # Track execution stats
    stats = {
        "start_time": datetime.now(),
        "dates_processed": [],
        "scopes_processed": [],
        "total_rows": 0,
        "errors": []
    }
    
    try:
        # ═══════════════════════════════════════════════════════════════════
        # STEP 1: Load Configuration
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 1: Loading configuration...")
        logger.info("=" * 60)
        
        config = load_config()
        logger.info(f"Configured scopes: {len(config.scopes)}")
        logger.info(f"Storage mode: {config.storage_mode.upper()}")
        logger.info(f"Output path: {config.output_path}")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 2: Authenticate with Azure
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 2: Authenticating with Azure AD...")
        logger.info("=" * 60)
        
        authenticator = AzureAuthenticator(config.azure, verify_ssl=config.verify_ssl)
        # Trigger authentication to validate credentials early
        _ = authenticator.token
        logger.info("Authentication successful")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 3: Initialize Components
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 3: Initializing pipeline components...")
        logger.info("=" * 60)
        
        extractor = AzureCostExtractor(
            authenticator=authenticator,
            poll_interval=config.poll_interval,
            max_poll_attempts=config.max_poll_attempts,
            request_timeout=config.request_timeout,
            download_timeout=config.download_timeout
        )
        
        # Initialize writer based on storage mode
        if config.storage_mode == "csv":
            from csv_writer import CSVWriter
            writer = CSVWriter(config.output_path)
            logger.info("Using CSV writer (local testing mode)")
        else:
            from delta_writer import DeltaLakeWriter
            writer = DeltaLakeWriter(config.output_path)
            logger.info("Using Delta Lake writer (production mode)")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 4: Process Each Date
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 4: Extracting and loading cost data...")
        logger.info("=" * 60)
        
        # Handle date range mode (start_date + end_date)
        if start_date and end_date:
            logger.info(f"\n{'─' * 40}")
            logger.info(f"Processing date range: {start_date} to {end_date}")
            logger.info(f"{'─' * 40}")
            
            try:
                # Extract data for the entire date range
                cost_reports = extractor.extract_costs_for_date(
                    scopes=config.scopes,
                    target_date=start_date,
                    end_date=end_date
                )
                
                if not cost_reports:
                    logger.warning(f"No data extracted for {start_date} to {end_date}")
                else:
                    # Write to storage (CSV or Delta Lake)
                    rows_written = writer.write_cost_data(
                        cost_reports=cost_reports,
                        use_merge=use_merge
                    )
                    
                    stats["dates_processed"].append(f"{start_date} to {end_date}")
                    stats["total_rows"] += rows_written
                    
                    for report in cost_reports:
                        if report.scope_name not in stats["scopes_processed"]:
                            stats["scopes_processed"].append(report.scope_name)
                            
            except Exception as e:
                error_msg = f"Error processing {start_date} to {end_date}: {str(e)}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
        
        else:
            # Single date mode (original behavior)
            # Default to yesterday if no dates provided
            if not target_dates:
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                target_dates = [yesterday]
            
            for target_date in target_dates:
                logger.info(f"\n{'─' * 40}")
                logger.info(f"Processing date: {target_date}")
                logger.info(f"{'─' * 40}")
                
                try:
                    # Extract data from all scopes
                    cost_reports = extractor.extract_costs_for_date(
                        scopes=config.scopes,
                        target_date=target_date
                    )
                    
                    if not cost_reports:
                        logger.warning(f"No data extracted for {target_date}")
                        continue
                    
                    # Write to storage (CSV or Delta Lake)
                    rows_written = writer.write_cost_data(
                        cost_reports=cost_reports,
                        use_merge=use_merge
                    )
                    
                    stats["dates_processed"].append(target_date)
                    stats["total_rows"] += rows_written
                    
                    # Track unique scopes
                    for report in cost_reports:
                        if report.scope_name not in stats["scopes_processed"]:
                            stats["scopes_processed"].append(report.scope_name)
                    
                except Exception as e:
                    error_msg = f"Error processing {target_date}: {str(e)}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
                    continue
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 5: Finalize
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 5: Pipeline completed")
        logger.info("=" * 60)
        
        stats["end_time"] = datetime.now()
        stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        stats["success"] = len(stats["errors"]) == 0
        
        # Print summary
        logger.info(f"\n{'═' * 60}")
        logger.info("PIPELINE SUMMARY")
        logger.info(f"{'═' * 60}")
        logger.info(f"  Status: {'SUCCESS' if stats['success'] else 'COMPLETED WITH ERRORS'}")
        logger.info(f"  Duration: {stats['duration_seconds']:.1f} seconds")
        logger.info(f"  Dates processed: {len(stats['dates_processed'])}")
        logger.info(f"  Scopes processed: {len(stats['scopes_processed'])}")
        logger.info(f"  Total rows written: {stats['total_rows']}")
        if stats["errors"]:
            logger.info(f"  Errors: {len(stats['errors'])}")
            for err in stats["errors"]:
                logger.error(f"    - {err}")
        logger.info(f"{'═' * 60}\n")
        
        return stats
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        stats["end_time"] = datetime.now()
        stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        stats["success"] = False
        stats["errors"].append(str(e))
        raise


def run_backfill(days: int, use_merge: bool = True) -> dict:
    """
    Run pipeline for multiple past days (backfill).
    
    Args:
        days: Number of days to backfill
        use_merge: If True, use MERGE for idempotency
        
    Returns:
        dict: Pipeline execution summary
    """
    dates = []
    for i in range(1, days + 1):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        dates.append(date)
    
    # Sort dates chronologically
    dates.sort()
    
    logger.info(f"Starting backfill for {len(dates)} days: {dates[0]} to {dates[-1]}")
    
    return run_pipeline(target_dates=dates, use_merge=use_merge)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Azure AI Foundry Cost Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                              # Process yesterday's data
  python main.py --date 2026-01-05            # Process specific date
  python main.py --days 7                     # Backfill last 7 days
  python main.py --lifetime                   # Get ALL available cost data (13 months)
  python main.py --start 2025-01-01 --end 2026-01-07  # Custom date range
  python main.py --no-merge                   # Use APPEND instead of MERGE
        """
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to process (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to backfill"
    )
    
    parser.add_argument(
        "--lifetime",
        action="store_true",
        help="Extract ALL available cost data (typically 13 months of history)"
    )
    
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for custom range (YYYY-MM-DD format, use with --end)"
    )
    
    parser.add_argument(
        "--end",
        type=str,
        help="End date for custom range (YYYY-MM-DD format, use with --start)"
    )
    
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Use APPEND instead of MERGE (may create duplicates)"
    )
    
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = parse_args()
    
    use_merge = not args.no_merge
    
    if args.lifetime:
        # Lifetime mode - get all available data (13 months back)
        from dateutil.relativedelta import relativedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - relativedelta(months=13)).strftime('%Y-%m-%d')
        logger.info(f"Lifetime mode: extracting data from {start_date} to {end_date}")
        result = run_pipeline(start_date=start_date, end_date=end_date, use_merge=use_merge)
    elif args.start and args.end:
        # Custom date range mode
        result = run_pipeline(start_date=args.start, end_date=args.end, use_merge=use_merge)
    elif args.days:
        # Backfill mode
        result = run_backfill(days=args.days, use_merge=use_merge)
    elif args.date:
        # Specific date mode
        result = run_pipeline(target_dates=[args.date], use_merge=use_merge)
    else:
        # Default: yesterday
        result = run_pipeline(use_merge=use_merge)
    
    # Exit with appropriate code
    sys.exit(0 if result.get("success", False) else 1)


# ═══════════════════════════════════════════════════════════════════════════
# DATABRICKS NOTEBOOK USAGE
# ═══════════════════════════════════════════════════════════════════════════
# 
# If running as a Databricks notebook, you can call the functions directly:
#
# # Run for yesterday
# result = run_pipeline()
#
# # Run for specific date
# result = run_pipeline(target_dates=["2026-01-05"])
#
# # Backfill last 7 days
# result = run_backfill(days=7)
#
# # Check result
# print(f"Rows written: {result['total_rows']}")
# ═══════════════════════════════════════════════════════════════════════════
