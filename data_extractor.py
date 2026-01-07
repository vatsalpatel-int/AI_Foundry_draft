"""
Azure Cost Data Extractor Module

Handles requesting, polling, and downloading cost reports from Azure Cost Management API.
Supports multiple scopes for extracting costs from different projects/subscriptions.
"""

import time
import logging
from dataclasses import dataclass
from typing import List, Optional

import requests
import urllib3

from auth import AzureAuthenticator

logger = logging.getLogger(__name__)

# Suppress SSL warnings if verification is disabled
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class CostReportData:
    """Container for extracted cost report data."""
    scope_id: str
    scope_name: str
    target_date: str
    csv_content: bytes
    row_count: Optional[int] = None


class AzureCostExtractor:
    """
    Extracts cost data from Azure Cost Management API.
    
    Supports multiple scopes (subscriptions, management groups, resource groups)
    for consolidated cost reporting across Azure AI Foundry projects.
    """
    
    # API endpoint template
    COST_REPORT_URL_TEMPLATE = (
        "https://management.azure.com/{scope_id}/"
        "providers/Microsoft.CostManagement/generateCostDetailsReport"
        "?api-version=2023-11-01"
    )
    
    def __init__(
        self,
        authenticator: AzureAuthenticator,
        poll_interval: int = 30,
        max_poll_attempts: int = 60,
        request_timeout: int = 60,
        download_timeout: int = 300
    ):
        """
        Initialize the cost extractor.
        
        Args:
            authenticator: Azure authenticator instance
            poll_interval: Seconds between status poll attempts
            max_poll_attempts: Maximum number of poll attempts before timeout
            request_timeout: Timeout for API requests in seconds
            download_timeout: Timeout for downloading reports in seconds
        """
        self._auth = authenticator
        self._poll_interval = poll_interval
        self._max_poll_attempts = max_poll_attempts
        self._request_timeout = request_timeout
        self._download_timeout = download_timeout
    
    def extract_costs_for_date(
        self,
        scopes: List[str],
        target_date: str,
        end_date: Optional[str] = None
    ) -> List[CostReportData]:
        """
        Extract cost data for all scopes for a specific date or date range.
        
        Args:
            scopes: List of Azure scope IDs
            target_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (defaults to target_date for single day)
            
        Returns:
            List[CostReportData]: Cost data for each scope
        """
        # Default end_date to target_date if not provided (single day)
        if end_date is None:
            end_date = target_date
        
        results = []
        
        for scope_id in scopes:
            scope_name = self._extract_scope_name(scope_id)
            logger.info(f"Processing scope: {scope_name}")
            
            try:
                csv_content = self._extract_single_scope(scope_id, target_date, end_date)
                
                # Use date range for the label
                date_label = target_date if target_date == end_date else f"{target_date}_to_{end_date}"
                
                results.append(CostReportData(
                    scope_id=scope_id,
                    scope_name=scope_name,
                    target_date=date_label,
                    csv_content=csv_content
                ))
                
                logger.info(f"Successfully extracted data for {scope_name}")
                
            except Exception as e:
                logger.error(f"Failed to extract data for {scope_name}: {str(e)}")
                # Continue with other scopes even if one fails
                continue
        
        return results
    
    def _extract_single_scope(self, scope_id: str, start_date: str, end_date: str) -> bytes:
        """
        Extract cost data for a single scope.
        
        Args:
            scope_id: Azure scope ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            bytes: Raw CSV content
        """
        # Step 1: Request report generation
        status_url = self._request_report(scope_id, start_date, end_date)
        
        # Step 2: Poll until ready
        download_url = self._wait_for_report(status_url)
        
        # Step 3: Download the report
        csv_content = self._download_report(download_url)
        
        return csv_content
    
    def _request_report(self, scope_id: str, start_date: str, end_date: str) -> str:
        """
        Request Azure to generate a cost details report.
        
        Args:
            scope_id: Azure scope ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            str: Status URL for polling
            
        Raises:
            requests.RequestException: If request fails
            ValueError: If no status URL returned
        """
        url = self.COST_REPORT_URL_TEMPLATE.format(scope_id=scope_id)
        
        payload = {
            "metric": "ActualCost",
            "timePeriod": {
                "start": start_date,
                "end": end_date
            }
        }
        
        date_range = start_date if start_date == end_date else f"{start_date} to {end_date}"
        logger.info(f"Requesting cost report for {date_range}...")
        
        response = requests.post(
            url,
            headers=self._auth.get_auth_headers(),
            json=payload,
            timeout=self._request_timeout,
            verify=self._auth.verify_ssl
        )
        response.raise_for_status()
        
        status_url = response.headers.get('Location')
        if not status_url:
            raise ValueError("No Location header in response - report request may have failed")
        
        logger.info("Report generation initiated")
        return status_url
    
    def _wait_for_report(self, status_url: str) -> str:
        """
        Poll until the report is ready for download.
        
        Args:
            status_url: URL to poll for status
            
        Returns:
            str: Download URL for the report
            
        Raises:
            TimeoutError: If report doesn't complete in time
            RuntimeError: If report generation fails
        """
        headers = self._auth.get_auth_headers()
        
        for attempt in range(self._max_poll_attempts):
            response = requests.get(
                status_url,
                headers=headers,
                timeout=self._request_timeout,
                verify=self._auth.verify_ssl
            )
            response.raise_for_status()
            
            status_data = response.json()
            status = status_data.get('status')
            
            if status == 'Completed':
                download_url = status_data['manifest']['blobs'][0]['blobLink']
                logger.info("Report generation completed")
                return download_url
            
            elif status == 'Failed':
                error_msg = status_data.get('error', {}).get('message', 'Unknown error')
                raise RuntimeError(f"Report generation failed: {error_msg}")
            
            logger.info(
                f"Report status: {status} - waiting {self._poll_interval}s "
                f"(attempt {attempt + 1}/{self._max_poll_attempts})"
            )
            time.sleep(self._poll_interval)
        
        raise TimeoutError(
            f"Report did not complete after {self._max_poll_attempts} attempts "
            f"({self._max_poll_attempts * self._poll_interval} seconds)"
        )
    
    def _download_report(self, download_url: str) -> bytes:
        """
        Download the cost report CSV.
        
        Args:
            download_url: URL to download from
            
        Returns:
            bytes: Raw CSV content
        """
        logger.info("Downloading report...")
        
        response = requests.get(download_url, timeout=self._download_timeout, verify=self._auth.verify_ssl)
        response.raise_for_status()
        
        content_size = len(response.content)
        logger.info(f"Downloaded {content_size / 1024:.1f} KB")
        
        return response.content
    
    @staticmethod
    def _extract_scope_name(scope_id: str) -> str:
        """
        Extract a human-readable name from a scope ID.
        
        Args:
            scope_id: Full Azure scope ID
            
        Returns:
            str: Short name for the scope
        """
        # Handle different scope formats
        parts = scope_id.split('/')
        
        if 'subscriptions' in scope_id:
            # subscriptions/{sub-id} or subscriptions/{sub-id}/resourceGroups/{rg}
            try:
                sub_idx = parts.index('subscriptions')
                return f"subscription-{parts[sub_idx + 1][:8]}"
            except (ValueError, IndexError):
                pass
        
        if 'managementGroups' in scope_id:
            # providers/Microsoft.Management/managementGroups/{mg-id}
            try:
                mg_idx = parts.index('managementGroups')
                return f"mg-{parts[mg_idx + 1]}"
            except (ValueError, IndexError):
                pass
        
        # Fallback: return last non-empty part
        return parts[-1] if parts[-1] else scope_id[:20]
