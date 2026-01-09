"""
Azure Authentication Module

Supports multiple authentication modes:
- CLI: Uses Azure CLI login (requires `az login`)
- Browser: Uses interactive browser login (development)
- Service Principal: Uses client credentials flow (production)

Provides token management for Azure Management API access.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from abc import ABC, abstractmethod

import requests
import urllib3

logger = logging.getLogger(__name__)

# Suppress SSL warnings if verification is disabled (for corporate proxies)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseAuthenticator(ABC):
    """Abstract base class for Azure authenticators."""
    
    # Azure Management API scope
    MANAGEMENT_SCOPE = "https://management.azure.com/.default"
    
    def __init__(self, verify_ssl: bool = True):
        self._verify_ssl = verify_ssl
    
    @abstractmethod
    def get_token(self) -> str:
        """Get a valid access token."""
        pass
    
    def get_auth_headers(self) -> dict:
        """
        Get authorization headers for API requests.
        
        Returns:
            dict: Headers with Bearer token
        """
        return {
            'Authorization': f'Bearer {self.get_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    @property
    def verify_ssl(self) -> bool:
        """Get SSL verification setting."""
        return self._verify_ssl
    
    def invalidate_token(self) -> None:
        """Force token refresh on next request (override in subclass if needed)."""
        pass


class AzureCliAuthenticator(BaseAuthenticator):
    """
    Authenticates using Azure CLI credentials.
    
    Requires: User must be logged in via `az login`
    Best for: Development and testing
    """
    
    def __init__(self, verify_ssl: bool = True):
        super().__init__(verify_ssl)
        self._credential = None
        
        if not verify_ssl:
            logger.warning("SSL verification is DISABLED - use only for testing behind corporate proxies")
        
        logger.info("Using Azure CLI authentication mode")
    
    def _get_credential(self):
        """Lazy load the Azure CLI credential."""
        if self._credential is None:
            try:
                from azure.identity import AzureCliCredential
                self._credential = AzureCliCredential()
            except ImportError:
                raise ImportError(
                    "azure-identity package is required for CLI authentication. "
                    "Install it with: pip install azure-identity"
                )
        return self._credential
    
    def get_token(self) -> str:
        """
        Get access token using Azure CLI credentials.
        
        Returns:
            str: Valid Azure access token
        """
        credential = self._get_credential()
        token = credential.get_token(self.MANAGEMENT_SCOPE)
        logger.debug("Retrieved token from Azure CLI")
        return token.token
    
    # Token is managed by Azure CLI, no invalidation needed
    def invalidate_token(self) -> None:
        """Azure CLI handles token refresh automatically."""
        logger.debug("Token invalidation requested (handled by Azure CLI)")


class InteractiveBrowserAuthenticator(BaseAuthenticator):
    """
    Authenticates using Interactive Browser credentials.
    
    Requires: User interaction (pops up browser)
    Best for: Local development without Azure CLI
    """
    
    def __init__(self, verify_ssl: bool = True):
        super().__init__(verify_ssl)
        self._credential = None
        
        if not verify_ssl:
            logger.warning("SSL verification is DISABLED - use only for testing behind corporate proxies")
        
        logger.info("Using Interactive Browser authentication mode")
    
    def _get_credential(self):
        """Lazy load the Interactive Browser credential."""
        if self._credential is None:
            try:
                from azure.identity import InteractiveBrowserCredential
                self._credential = InteractiveBrowserCredential()
            except ImportError:
                raise ImportError(
                    "azure-identity package is required for browser authentication. "
                    "Install it with: pip install azure-identity"
                )
        return self._credential
    
    def get_token(self) -> str:
        """
        Get access token using Interactive Browser credentials.
        
        Returns:
            str: Valid Azure access token
        """
        credential = self._get_credential()
        # This will open a browser window if not cached
        token = credential.get_token(self.MANAGEMENT_SCOPE)
        logger.debug("Retrieved token from Interactive Browser")
        return token.token
    
    def invalidate_token(self) -> None:
        """Force token refresh."""
        # InteractiveBrowserCredential caches internally, but we can't easily clear it
        # without user interaction. Rely on MSAL's internal expiration handling.
        pass


class ServicePrincipalAuthenticator(BaseAuthenticator):
    """
    Authenticates using Service Principal (client credentials flow).
    
    Requires: Tenant ID, Client ID, Client Secret
    Best for: Production, automated pipelines, scheduled jobs
    """
    
    # Token endpoint template
    TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        timeout: int = 30,
        verify_ssl: bool = True
    ):
        """
        Initialize the Service Principal authenticator.
        
        Args:
            tenant_id: Azure AD Tenant ID
            client_id: Service Principal Application (Client) ID
            client_secret: Service Principal Client Secret
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        super().__init__(verify_ssl)
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout = timeout
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        
        if not verify_ssl:
            logger.warning("SSL verification is DISABLED - use only for testing behind corporate proxies")
        
        logger.info("Using Service Principal authentication mode")
    
    def get_token(self) -> str:
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
        url = self.TOKEN_URL_TEMPLATE.format(tenant_id=self._tenant_id)
        
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'scope': self.MANAGEMENT_SCOPE
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
    
    def invalidate_token(self) -> None:
        """Force token refresh on next request."""
        self._token = None
        self._token_expiry = None
        logger.info("Token invalidated - will refresh on next request")


# ═══════════════════════════════════════════════════════════════════════════
# BACKWARD COMPATIBILITY ALIAS
# ═══════════════════════════════════════════════════════════════════════════
# This allows existing code using AzureAuthenticator to continue working

class AzureAuthenticator(ServicePrincipalAuthenticator):
    """
    Backward-compatible alias for ServicePrincipalAuthenticator.
    
    Deprecated: Use ServicePrincipalAuthenticator or AzureCliAuthenticator directly.
    """
    
    def __init__(self, config, timeout: int = 30, verify_ssl: bool = True):
        """
        Initialize from AzureConfig object (backward compatible).
        
        Args:
            config: AzureConfig object with tenant_id, client_id, client_secret
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        super().__init__(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
            timeout=timeout,
            verify_ssl=verify_ssl
        )
    
    @property
    def token(self) -> str:
        """Backward-compatible token property."""
        return self.get_token()


def create_authenticator(auth_mode: str, config=None, verify_ssl: bool = True) -> BaseAuthenticator:
    """
    Factory function to create the appropriate authenticator.
    
    Args:
        auth_mode: "cli", "browser", or "service_principal"
        config: AzureConfig object (required for service_principal mode)
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        BaseAuthenticator: Configured authenticator instance
        
    Raises:
        ValueError: If auth_mode is invalid or config is missing for service_principal
    """
    auth_mode = auth_mode.lower().strip()
    
    if auth_mode == "cli":
        return AzureCliAuthenticator(verify_ssl=verify_ssl)
    
    elif auth_mode == "browser":
        return InteractiveBrowserAuthenticator(verify_ssl=verify_ssl)
    
    elif auth_mode == "service_principal":
        if config is None:
            raise ValueError("config is required for service_principal authentication mode")
        return ServicePrincipalAuthenticator(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
            verify_ssl=verify_ssl
        )
    
    else:
        raise ValueError(
            f"Invalid AUTH_MODE: '{auth_mode}'. "
            f"Valid options are: 'cli', 'browser', 'service_principal'"
        )
