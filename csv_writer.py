"""
CSV Writer Module (Local Testing)

Handles writing cost data to local CSV files for testing purposes.
This is a simplified alternative to Delta Lake for local development.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import List

from data_extractor import CostReportData

logger = logging.getLogger(__name__)


class CSVWriter:
    """
    Writes cost data to local CSV files for testing.
    
    Features:
    - Saves raw CSV from Azure
    - Adds metadata columns
    - Organizes by date
    - Supports append mode
    """
    
    def __init__(self, output_dir: str):
        """
        Initialize the CSV writer.
        
        Args:
            output_dir: Directory to save CSV files
        """
        self._output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(self._output_dir, exist_ok=True)
        logger.info(f"CSV output directory: {self._output_dir}")
    
    def write_cost_data(
        self,
        cost_reports: List[CostReportData],
        use_merge: bool = True  # Ignored for CSV, kept for compatibility
    ) -> int:
        """
        Write cost report data to CSV files.
        
        Args:
            cost_reports: List of cost report data objects
            use_merge: Ignored (kept for interface compatibility)
            
        Returns:
            int: Total number of rows written
        """
        total_rows = 0
        
        for report in cost_reports:
            logger.info(f"Processing data for scope: {report.scope_name}")
            
            try:
                # Load CSV content into pandas DataFrame
                df = self._load_csv_to_dataframe(report)
                
                if df is None or len(df) == 0:
                    logger.warning(f"No data found for scope {report.scope_name}")
                    continue
                
                # Add metadata columns
                df = self._add_metadata_columns(df, report)
                
                # Save to CSV
                row_count = len(df)
                self._save_to_csv(df, report)
                
                total_rows += row_count
                logger.info(f"Wrote {row_count} rows for {report.scope_name}")
                
            except Exception as e:
                logger.error(f"Error processing {report.scope_name}: {str(e)}")
                continue
        
        logger.info(f"Total rows written: {total_rows}")
        return total_rows
    
    def _load_csv_to_dataframe(self, report: CostReportData) -> pd.DataFrame:
        """
        Load CSV content into a pandas DataFrame.
        
        Args:
            report: Cost report data with CSV content
            
        Returns:
            pd.DataFrame: DataFrame with cost data
        """
        import io
        
        # Decode bytes to string and read as CSV
        csv_string = report.csv_content.decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_string))
        
        logger.info(f"Loaded {len(df)} rows from CSV")
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame, report: CostReportData) -> pd.DataFrame:
        """
        Add metadata columns for data lineage.
        
        Args:
            df: pandas DataFrame
            report: Cost report data
            
        Returns:
            pd.DataFrame: DataFrame with additional metadata columns
        """
        df['_source_scope'] = report.scope_id
        df['_source_scope_name'] = report.scope_name
        df['_ingestion_timestamp'] = datetime.now().isoformat()
        df['_ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
        df['_cost_date'] = report.target_date
        
        return df
    
    def _save_to_csv(self, df: pd.DataFrame, report: CostReportData) -> str:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: pandas DataFrame to save
            report: Cost report data for naming
            
        Returns:
            str: Path to saved file
        """
        # Create filename with date and scope
        safe_scope_name = report.scope_name.replace('/', '_').replace('\\', '_')
        filename = f"cost_data_{report.target_date}_{safe_scope_name}.csv"
        filepath = os.path.join(self._output_dir, filename)
        
        # Check if file exists (for append mode)
        if os.path.exists(filepath):
            # Append to existing file
            existing_df = pd.read_csv(filepath)
            df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates based on key columns
            key_columns = ['Date', 'ResourceId', 'MeterId', 'SubscriptionId', '_source_scope']
            available_keys = [col for col in key_columns if col in df.columns]
            if available_keys:
                df = df.drop_duplicates(subset=available_keys, keep='last')
            
            logger.info(f"Appended to existing file: {filepath}")
        else:
            logger.info(f"Creating new file: {filepath}")
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        
        logger.info(f"Saved {len(df)} rows to {filepath}")
        return filepath
    
    def get_output_stats(self) -> dict:
        """
        Get statistics about the output directory.
        
        Returns:
            dict: Output statistics
        """
        csv_files = [f for f in os.listdir(self._output_dir) if f.endswith('.csv')]
        
        total_rows = 0
        file_info = []
        
        for csv_file in csv_files:
            filepath = os.path.join(self._output_dir, csv_file)
            df = pd.read_csv(filepath)
            total_rows += len(df)
            file_info.append({
                "filename": csv_file,
                "rows": len(df),
                "size_kb": os.path.getsize(filepath) / 1024
            })
        
        return {
            "output_dir": self._output_dir,
            "file_count": len(csv_files),
            "total_rows": total_rows,
            "files": file_info
        }
