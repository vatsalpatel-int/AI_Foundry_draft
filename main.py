"""
Azure AI Foundry Cost Pipeline - Main Orchestrator

This is the main entry point for the cost extraction pipeline.
It orchestrates the flow:
    1. Load configuration
    2. Authenticate with Azure
    3. Extract cost data from all configured scopes
    4. Write to Delta Lake with idempotent MERGE

Designed to run in Databricks Runtime (DBR) environment.

Usage:
    - As Databricks notebook: Just run the cells
    - As script: python main.py [--date YYYY-MM-DD] [--days N] [--no-merge]
    
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
    use_merge: bool = True
) -> dict:
    """
    Main pipeline execution function.
    
    Args:
        target_dates: List of dates to process (YYYY-MM-DD format).
                     Defaults to yesterday if not provided.
        use_merge: If True, use MERGE for idempotency. If False, use APPEND.
        
    Returns:
        dict: Pipeline execution summary
    """
    # Import modules here to allow for cleaner error messages if dependencies missing
    from config import load_config
    from auth import AzureAuthenticator
    from data_extractor import AzureCostExtractor
    from delta_writer import DeltaLakeWriter
    
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
        logger.info(f"Delta table path: {config.delta_table_path}")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 2: Authenticate with Azure
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 2: Authenticating with Azure AD...")
        logger.info("=" * 60)
        
        authenticator = AzureAuthenticator(config.azure)
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
        
        writer = DeltaLakeWriter(config.delta_table_path)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 4: Process Each Date
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 60)
        logger.info("STEP 4: Extracting and loading cost data...")
        logger.info("=" * 60)
        
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
                
                # Write to Delta Lake
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
  python main.py                    # Process yesterday's data
  python main.py --date 2026-01-05  # Process specific date
  python main.py --days 7           # Backfill last 7 days
  python main.py --no-merge         # Use APPEND instead of MERGE
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
        help="Number of days to backfill (mutually exclusive with --date)"
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
    
    if args.days:
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
