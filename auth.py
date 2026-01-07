"""
Azure Authentication Module

Handles OAuth 2.0 authentication with Azure AD using client credentials flow.
Provides token management for Azure Management API access.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
import urllib3

from config import AzureConfig

logger = logging.getLogger(__name__)

# Suppress SSL warnings if verification is disabled (for corporate proxies)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AzureAuthenticator:
    """
    Handles Azure AD authentication using client credentials flow.
    
    Caches tokens and automatically refreshes when expired.
    """
    
    # Token endpoint template
    TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    
    # Azure Management API resource
    MANAGEMENT_RESOURCE = "https://management.azure.com/"
    
    def __init__(self, config: AzureConfig, timeout: int = 30, verify_ssl: bool = True):
        """
        Initialize the authenticator.
        
        Args:
            config: Azure configuration with credentials
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates (set False for corporate proxies)
        """
        self._config = config
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        
        if not verify_ssl:
            logger.warning("SSL verification is DISABLED - use only for testing behind corporate proxies")
    
    @property
    def token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            str: Valid Azure access token
        """
        if self._is_token_valid():
            return self._token
        
        return self._fetch_new_token()
    
    def _is_token_valid(self) -> bool:
        """Check if the cached token is still valid (with 5-minute buffer)."""
        if self._token is None or self._token_expiry is None:
            return False
        
        # Refresh 5 minutes before expiry to be safe
        buffer = timedelta(minutes=5)
        return datetime.now() < (self._token_expiry - buffer)
    
    def _fetch_new_token(self) -> str:
        """
        Fetch a new access token from Azure AD.
        
        Returns:
            str: New access token
            
        Raises:
            requests.RequestException: If authentication fails
            ValueError: If response is invalid
        """
        url = self.TOKEN_URL_TEMPLATE.format(tenant_id=self._config.tenant_id)
        
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self._config.client_id,
            'client_secret': self._config.client_secret,
            'resource': self.MANAGEMENT_RESOURCE
        }
        
        logger.info("Requesting new Azure access token...")
        
        response = requests.post(url, data=payload, timeout=self._timeout, verify=self._verify_ssl)
        response.raise_for_status()
        
        data = response.json()
        
        if 'access_token' not in data:
            raise ValueError("No access_token in authentication response")
        
        self._token = data['access_token']
        
        # Calculate expiry time (Azure tokens typically last 1 hour)
        expires_in = int(data.get('expires_in', 3600))
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
        
        logger.info(f"Successfully authenticated. Token expires in {expires_in} seconds")
        
        return self._token
    
    def get_auth_headers(self) -> dict:
        """
        Get authorization headers for API requests.
        
        Returns:
            dict: Headers with Bearer token
        """
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    @property
    def verify_ssl(self) -> bool:
        """Get SSL verification setting."""
        return self._verify_ssl
    
    def invalidate_token(self) -> None:
        """Force token refresh on next request."""
        self._token = None
        self._token_expiry = None
        logger.info("Token invalidated - will refresh on next request")
