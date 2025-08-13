"""
Abstract base class for API endpoint data processors.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List, Optional
import polars as pl
import logging
from datetime import datetime

try:
    from ..client import HTTPClient
    from ..config import Config
    from ..utils.xml_parser import parse_xml_response
except ImportError:
    from client import HTTPClient
    from config import Config
    from utils.xml_parser import parse_xml_response

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """Abstract base class for endpoint data processors."""
    
    def __init__(
        self,
        client: HTTPClient,
        config: Config,
        output_dir: Optional[Path] = None
    ):
        self.client = client
        self.config = config
        self.output_dir = output_dir or Path("./data")
        self.output_dir.mkdir(exist_ok=True)
        
    @property
    @abstractmethod
    def endpoint_name(self) -> str:
        """Name of the API endpoint this processor handles."""
        pass
        
    @abstractmethod
    def process_to_parquet(
        self, 
        params: Optional[Dict[str, Any]] = None,
        timestamp_suffix: bool = True
    ) -> List[Path]:
        """
        Process endpoint data and save to parquet files.
        
        Args:
            params: API parameters
            timestamp_suffix: Whether to add timestamp to filenames
            
        Returns:
            List of created parquet file paths
        """
        pass
        
    @abstractmethod
    def _transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, pl.DataFrame]:
        """
        Transform parsed XML data into structured DataFrames.
        
        Args:
            raw_data: Parsed XML data
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        pass
        
    async def _fetch_and_parse(
        self, 
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Fetch data from API and parse XML response.
        
        Args:
            params: API parameters
            
        Returns:
            Parsed data dictionary
        """
        # Get endpoint version and schema
        version = self.config.get_endpoint(self.endpoint_name).get_latest_version()
        schema = self.config.get_endpoint_schema(self.endpoint_name, version)
        
        # Add version to parameters
        request_params = {"version": version}
        if params:
            request_params.update(params)
            
        logger.info(f"Fetching {self.endpoint_name} with params: {request_params}")
        
        # Fetch data
        xml_response = await self.client.get(self.endpoint_name, request_params)
        
        # Parse XML
        parsed_data = parse_xml_response(xml_response, schema)
        
        return parsed_data
        
    def _save_dataframe(
        self,
        df: pl.DataFrame,
        filename: str,
        timestamp_suffix: bool = True
    ) -> Path:
        """
        Save DataFrame to parquet file.
        
        Args:
            df: DataFrame to save
            filename: Base filename (without extension)
            timestamp_suffix: Whether to add timestamp
            
        Returns:
            Path to saved file
        """
        if timestamp_suffix:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename}_{timestamp}"
            
        filepath = self.output_dir / f"{filename}.parquet"
        
        logger.info(f"Saving {len(df)} rows to {filepath}")
        df.write_parquet(filepath)
        
        return filepath
        
    def _validate_schema(self, df: pl.DataFrame, expected_columns: List[str]) -> None:
        """
        Validate DataFrame schema against expected columns.
        
        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
        """
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
            
        logger.debug(f"Schema validation passed for {len(df.columns)} columns")