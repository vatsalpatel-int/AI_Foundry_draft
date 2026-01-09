"""
Azure Cost Data Extractor Module

Handles requesting and processing cost data from Azure Cost Management Query API.
Supports multiple scopes for extracting costs from different projects/subscriptions.

Uses the Query API which supports all subscription types including Pay-As-You-Go (PAYG).
"""

import io
import csv
import time
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import requests
import urllib3

from auth import BaseAuthenticator

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
    Extracts cost data from Azure Cost Management Query API.
    
    Supports multiple scopes (subscriptions, management groups, resource groups)
    for consolidated cost reporting across Azure AI Foundry projects.
    
    Uses the Query API (supports PAYG) instead of generateCostDetailsReport (EA/MCA only).
    """
    
    # Query API endpoint template
    QUERY_URL_TEMPLATE = (
        "https://management.azure.com/{scope_id}/"
        "providers/Microsoft.CostManagement/query"
        "?api-version=2025-03-01"
    )
    
    # Retry configuration
    MAX_RETRIES = 5
    INITIAL_BACKOFF_SECONDS = 2
    MAX_BACKOFF_SECONDS = 120
    
    def __init__(
        self,
        authenticator: BaseAuthenticator,
        poll_interval: int = 30,
        max_poll_attempts: int = 60,
        request_timeout: int = 60,
        download_timeout: int = 300
    ):
        """
        Initialize the cost extractor.
        
        Args:
            authenticator: Azure authenticator instance (CLI or Service Principal)
            poll_interval: Unused (kept for backward compatibility)
            max_poll_attempts: Unused (kept for backward compatibility)
            request_timeout: Timeout for API requests in seconds
            download_timeout: Timeout for pagination requests in seconds
        """
        self._auth = authenticator
        self._poll_interval = poll_interval  # Unused but kept for compatibility
        self._max_poll_attempts = max_poll_attempts  # Unused but kept for compatibility
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
                csv_content, row_count = self._extract_single_scope(scope_id, target_date, end_date)
                
                # Use date range for the label
                date_label = target_date if target_date == end_date else f"{target_date}_to_{end_date}"
                
                results.append(CostReportData(
                    scope_id=scope_id,
                    scope_name=scope_name,
                    target_date=date_label,
                    csv_content=csv_content,
                    row_count=row_count
                ))
                
                logger.info(f"Successfully extracted {row_count} rows for {scope_name}")
                
            except Exception as e:
                logger.error(f"Failed to extract data for {scope_name}: {str(e)}")
                # Continue with other scopes even if one fails
                continue
        
        return results
    
    def _extract_single_scope(self, scope_id: str, start_date: str, end_date: str) -> tuple:
        """
        Extract cost data for a single scope using Query API.
        
        Args:
            scope_id: Azure scope ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            tuple: (csv_content as bytes, row_count)
        """
        # Step 1: Make Query API request
        all_rows = []
        columns = None
        
        response_data = self._make_query_request(scope_id, start_date, end_date)
        columns = response_data["properties"]["columns"]
        all_rows.extend(response_data["properties"].get("rows", []))
        
        logger.info(f"Initial query returned {len(response_data['properties'].get('rows', []))} rows")
        
        # Step 2: Handle pagination via nextLink
        page_count = 1
        while response_data["properties"].get("nextLink"):
            page_count += 1
            logger.info(f"Fetching page {page_count}...")
            response_data = self._fetch_next_page(response_data["properties"]["nextLink"])
            new_rows = response_data["properties"].get("rows", [])
            all_rows.extend(new_rows)
            logger.info(f"Page {page_count} returned {len(new_rows)} rows")
        
        logger.info(f"Total rows collected: {len(all_rows)} across {page_count} page(s)")
        
        # Step 3: Convert to CSV bytes (maintains compatibility with writers)
        csv_content = self._convert_to_csv(columns, all_rows)
        
        return csv_content, len(all_rows)
    
    def _request_with_retry(
        self,
        method: str,
        url: str,
        payload: Optional[Dict] = None,
        timeout: int = 60
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic for transient errors.
        
        Handles:
        - 401 Unauthorized: Refreshes token and retries
        - 429 Too Many Requests: Exponential backoff
        - 5xx Server Errors: Exponential backoff
        
        Args:
            method: HTTP method ('GET' or 'POST')
            url: Request URL
            payload: JSON payload for POST requests
            timeout: Request timeout in seconds
            
        Returns:
            requests.Response: Successful response
            
        Raises:
            requests.RequestException: If all retries fail
        """
        last_exception = None
        backoff = self.INITIAL_BACKOFF_SECONDS
        
        for attempt in range(self.MAX_RETRIES):
            try:
                # Always get fresh headers (handles token refresh)
                headers = self._auth.get_auth_headers()
                
                if method.upper() == 'POST':
                    response = requests.post(
                        url,
                        headers=headers,
                        json=payload,
                        timeout=timeout,
                        verify=self._auth.verify_ssl
                    )
                else:
                    response = requests.get(
                        url,
                        headers=headers,
                        timeout=timeout,
                        verify=self._auth.verify_ssl
                    )
                
                # Check for retryable status codes
                if response.status_code == 401:
                    # Token expired or invalid - invalidate and retry
                    logger.warning(f"401 Unauthorized - refreshing token (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    logger.debug(f"Response: {response.text[:500]}")
                    self._auth.invalidate_token()
                    time.sleep(1)  # Brief pause before retry
                    continue
                
                elif response.status_code == 429:
                    # Rate limited - use Retry-After header if available
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = min(backoff, self.MAX_BACKOFF_SECONDS)
                    
                    logger.warning(
                        f"429 Too Many Requests - waiting {wait_time}s "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                    backoff *= 2  # Exponential backoff
                    continue
                
                elif response.status_code >= 500:
                    # Server error - retry with backoff
                    wait_time = min(backoff, self.MAX_BACKOFF_SECONDS)
                    logger.warning(
                        f"{response.status_code} Server Error - waiting {wait_time}s "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                    backoff *= 2
                    continue
                
                # Success or non-retryable error
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                wait_time = min(backoff, self.MAX_BACKOFF_SECONDS)
                logger.warning(
                    f"Request timeout - waiting {wait_time}s "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                time.sleep(wait_time)
                backoff *= 2
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                wait_time = min(backoff, self.MAX_BACKOFF_SECONDS)
                logger.warning(
                    f"Connection error - waiting {wait_time}s "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                time.sleep(wait_time)
                backoff *= 2
        
        # All retries exhausted
        if last_exception:
            raise last_exception
        else:
            raise requests.RequestException(f"Request failed after {self.MAX_RETRIES} attempts")
    
    def _make_query_request(self, scope_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Make the initial Query API request.
        
        Args:
            scope_id: Azure scope ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            dict: JSON response from Query API
            
        Raises:
            requests.RequestException: If request fails
        """
        url = self.QUERY_URL_TEMPLATE.format(scope_id=scope_id)
        
        # Query API request body
        payload = {
            "type": "Usage",
            "timeframe": "Custom",
            "timePeriod": {
                "from": f"{start_date}T00:00:00+00:00",
                "to": f"{end_date}T23:59:59+00:00"
            },
            "dataset": {
                "granularity": "Daily"
            }
        }
        
        date_range = start_date if start_date == end_date else f"{start_date} to {end_date}"
        logger.info(f"Querying cost data for {date_range}...")
        
        response = self._request_with_retry(
            method='POST',
            url=url,
            payload=payload,
            timeout=self._request_timeout
        )
        
        return response.json()
    
    def _fetch_next_page(self, next_link: str) -> Dict[str, Any]:
        """
        Fetch the next page of results using nextLink.
        
        Args:
            next_link: URL for the next page of results
            
        Returns:
            dict: JSON response from Query API
        """
        response = self._request_with_retry(
            method='GET',
            url=next_link,
            timeout=self._download_timeout
        )
        
        return response.json()
    
    def _convert_to_csv(self, columns: List[Dict], rows: List[List]) -> bytes:
        """
        Convert Query API JSON response to CSV bytes.
        
        This maintains compatibility with existing csv_writer and delta_writer
        which expect CSV content.
        
        Args:
            columns: List of column definitions from API response
            rows: List of row data from API response
            
        Returns:
            bytes: CSV content as bytes
        """
        # Extract column names
        column_names = [col["name"] for col in columns]
        
        # Write to CSV in memory
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        writer.writerow(column_names)
        
        # Write data rows
        for row in rows:
            writer.writerow(row)
        
        # Convert to bytes
        csv_content = output.getvalue().encode('utf-8')
        
        logger.info(f"Converted {len(rows)} rows to CSV ({len(csv_content) / 1024:.1f} KB)")
        
        return csv_content
    
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
