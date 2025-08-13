"""
Configuration management for endpoint definitions and validation.
"""

import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path
from pydantic import BaseModel, Field


class EndpointParameter(BaseModel):
    """Model for endpoint parameter definition."""
    name: str
    required: bool = False
    

class EndpointVersion(BaseModel):
    """Model for endpoint version configuration."""
    parameters: List[str] = Field(default_factory=list)
    api_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    

class EndpointConfig(BaseModel):
    """Model for complete endpoint configuration."""
    version: Dict[str, EndpointVersion]
    
    def get_latest_version(self) -> str:
        """Get the latest version number."""
        versions = [float(v) for v in self.version.keys()]
        return str(max(versions))
        

class Config:
    """Configuration loader and validator for YAML endpoint definitions."""
    
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = Path(__file__).parent / "endpoints.yaml"
        
        self.config_path = Path(config_path)
        self._endpoints: Dict[str, EndpointConfig] = {}
        self._load_config()
        
    def _load_config(self) -> None:
        """Load and validate YAML configuration."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
        with open(self.config_path, 'r', encoding='utf-8') as file:
            raw_config = yaml.safe_load(file)
            
        # Store definitions
        self.definitions = raw_config.get('definitions', {})
        
        # Parse and validate each endpoint
        # First try new nested structure under 'endpoints'
        endpoints_data = raw_config.get('endpoints', {})
        for endpoint_name, endpoint_data in endpoints_data.items():
            self._endpoints[endpoint_name] = EndpointConfig(**endpoint_data)
            
        # Then add top-level endpoints for backward compatibility
        for key, value in raw_config.items():
            if key not in ['definitions', 'endpoints'] and isinstance(value, dict) and 'version' in value:
                self._endpoints[key] = EndpointConfig(**value)
            
    def get_endpoint(self, name: str) -> EndpointConfig:
        """Get endpoint configuration by name."""
        if name not in self._endpoints:
            available = list(self._endpoints.keys())
            raise ValueError(f"Unknown endpoint '{name}'. Available: {available}")
        return self._endpoints[name]
        
    def list_endpoints(self) -> List[str]:
        """Get list of available endpoint names."""
        return list(self._endpoints.keys())
        
    def get_endpoint_schema(self, name: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Get schema for specific endpoint version."""
        endpoint = self.get_endpoint(name)
        
        if version is None:
            version = endpoint.get_latest_version()
            
        if version not in endpoint.version:
            available = list(endpoint.version.keys())
            raise ValueError(f"Version '{version}' not found for '{name}'. Available: {available}")
            
        version_config = endpoint.version[version]
        
        if not version_config.api_schema:
            raise ValueError(f"No api_schema found for endpoint '{name}' version '{version}'")
            
        return version_config.api_schema
        
    def get_endpoint_parameters(self, name: str, version: Optional[str] = None) -> List[str]:
        """Get parameters for specific endpoint version."""
        endpoint = self.get_endpoint(name)
        
        if version is None:
            version = endpoint.get_latest_version()
            
        return endpoint.version[version].parameters
        
    def get_definitions(self) -> Dict[str, Any]:
        """Get definitions section from config."""
        return self.definitions
        
    def get_output_schema(self, name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get output schema for endpoint processing."""
        endpoint = self.get_endpoint(name)
        
        if version is None:
            version = endpoint.get_latest_version()
            
        if version not in endpoint.version:
            available = list(endpoint.version.keys())
            raise ValueError(f"Version '{version}' not found for '{name}'. Available: {available}")
            
        return endpoint.version[version].output_schema