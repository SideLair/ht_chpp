"""
HTTP Client for Hattrick API with OAuth authentication and connection pooling.
"""

import asyncio
import logging
from typing import Dict, Optional, Any
import aiohttp
import requests
from authlib.integrations.requests_client import OAuth1Session

logger = logging.getLogger(__name__)


class HTTPClient:
    """Async HTTP client for Hattrick API with OAuth1 authentication."""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token: str,
        token_secret: str,
        base_url: str = "https://chpp.hattrick.org/chppxml.ashx",
        max_connections: int = 50,
        rate_limit_per_second: int = 10
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = token
        self.token_secret = token_secret
        self.base_url = base_url
        self.max_connections = max_connections
        self.rate_limit_per_second = rate_limit_per_second
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(rate_limit_per_second)
        
    async def __aenter__(self):
        """Async context manager entry."""
        connector = aiohttp.TCPConnector(limit=self.max_connections)
        self._session = aiohttp.ClientSession(connector=connector)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()
            
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        Make authenticated GET request to Hattrick API.
        
        Args:
            endpoint: API endpoint (e.g., 'worlddetails')
            params: Additional parameters
            
        Returns:
            Raw XML response text
        """
        if not self._session:
            raise RuntimeError("Client not initialized. Use async context manager.")
            
        # Rate limiting
        async with self._semaphore:
            # Build parameters
            request_params = {"file": endpoint}
            if params:
                request_params.update(params)
                
            logger.debug(f"Making request to {endpoint} with params: {request_params}")
            
            # Use synchronous OAuth1 session for now (simpler implementation)
            oauth_session = OAuth1Session(
                client_id=self.client_id,
                client_secret=self.client_secret,
                token=self.token,
                token_secret=self.token_secret
            )
            
            # Make OAuth1 signed request
            response = oauth_session.get(self.base_url, params=request_params)
            
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
            return response.text
                
    async def get_multiple(
        self, 
        endpoint: str, 
        param_list: list[Dict[str, Any]]
    ) -> list[str]:
        """
        Make multiple parallel requests to the same endpoint.
        
        Args:
            endpoint: API endpoint
            param_list: List of parameter dictionaries
            
        Returns:
            List of XML responses
        """
        tasks = [self.get(endpoint, params) for params in param_list]
        return await asyncio.gather(*tasks)
        
    @classmethod
    def from_env(cls) -> "HTTPClient":
        """Create client from environment variables."""
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        return cls(
            client_id=os.getenv("HT_CLIENT_ID"),
            client_secret=os.getenv("HT_CLIENT_SECRET"), 
            token=os.getenv("HT_OAUTH_TOKEN"),
            token_secret=os.getenv("HT_OAUTH_TOKEN_SECRET")
        )