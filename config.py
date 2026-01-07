"""
Configuration Management Module

Handles loading and validating configuration from environment variables.
Supports multiple Azure scopes for multi-project cost extraction.
"""

import os
import sys
import logging
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class AzureConfig:
    """Azure authentication configuration."""
    tenant_id: str
    client_id: str
    client_secret: str


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    azure: AzureConfig
    scopes: List[str]  # Multiple scopes for different projects
    output_path: str  # Path for output (CSV directory or Delta table)
    storage_mode: str = "csv"  # "csv" for local testing, "delta" for production
    poll_interval: int = 30
    max_poll_attempts: int = 60
    request_timeout: int = 60
    download_timeout: int = 300


def load_config() -> PipelineConfig:
    """
    Load and validate configuration from environment variables.
    
    Returns:
        PipelineConfig: Validated configuration object
        
    Raises:
        SystemExit: If required configuration is missing
    """
    # Load .env file
    load_dotenv()
    
    # Required environment variables
    required_vars = {
        'AZURE_TENANT_ID': 'Azure AD Tenant ID',
        'AZURE_CLIENT_ID': 'Azure Service Principal Client ID',
        'AZURE_CLIENT_SECRET': 'Azure Service Principal Client Secret',
        'AZURE_SCOPES': 'Comma-separated list of Azure scopes',
        'OUTPUT_PATH': 'Output path (local directory for CSV or Delta table path)'
    }
    
    # Collect values and check for missing
    values = {}
    missing = []
    
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value or value.startswith('your-'):
            missing.append(f"  - {var}: {description}")
        values[var] = value
    
    if missing:
        logger.error("Missing or unconfigured environment variables:")
        for m in missing:
            logger.error(m)
        logger.error("\nPlease configure these in your .env file")
        sys.exit(1)
    
    # Parse scopes (comma-separated)
    scopes = [s.strip() for s in values['AZURE_SCOPES'].split(',') if s.strip()]
    
    if not scopes:
        logger.error("AZURE_SCOPES must contain at least one scope")
        sys.exit(1)
    
    # Get storage mode (csv for local testing, delta for production)
    storage_mode = os.getenv('STORAGE_MODE', 'csv').lower()
    if storage_mode not in ['csv', 'delta']:
        logger.warning(f"Invalid STORAGE_MODE '{storage_mode}', defaulting to 'csv'")
        storage_mode = 'csv'
    
    logger.info(f"Loaded configuration with {len(scopes)} scope(s)")
    logger.info(f"Storage mode: {storage_mode.upper()}")
    
    # Build configuration object
    config = PipelineConfig(
        azure=AzureConfig(
            tenant_id=values['AZURE_TENANT_ID'],
            client_id=values['AZURE_CLIENT_ID'],
            client_secret=values['AZURE_CLIENT_SECRET']
        ),
        scopes=scopes,
        output_path=values['OUTPUT_PATH'],
        storage_mode=storage_mode,
        poll_interval=int(os.getenv('POLL_INTERVAL', '30')),
        max_poll_attempts=int(os.getenv('MAX_POLL_ATTEMPTS', '60')),
        request_timeout=int(os.getenv('REQUEST_TIMEOUT', '60')),
        download_timeout=int(os.getenv('DOWNLOAD_TIMEOUT', '300'))
    )
    
    return config
