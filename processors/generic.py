"""
Generic processor that reads output_schema from YAML configuration.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import polars as pl
import logging

try:
    from .base import BaseProcessor
    from ..utils.types import create_table_schema
except ImportError:
    from base import BaseProcessor
    from utils.types import create_table_schema

logger = logging.getLogger(__name__)


class GenericProcessor(BaseProcessor):
    """
    Generic processor that transforms endpoint data based on output_schema in YAML config.
    
    Can handle any endpoint that has output_schema defined in endpoints.yaml
    """
    
    def __init__(self, endpoint_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._endpoint_name = endpoint_name
    
    @property
    def endpoint_name(self) -> str:
        return self._endpoint_name
        
    async def process_to_parquet(
        self, 
        params: Optional[Dict[str, Any]] = None,
        timestamp_suffix: bool = True
    ) -> List[Path]:
        """
        Process endpoint data based on output_schema configuration.
        
        Args:
            params: API parameters
            timestamp_suffix: Whether to add timestamp to filenames
            
        Returns:
            List of created parquet file paths
        """
        logger.info(f"Processing {self.endpoint_name} endpoint with generic processor")
        
        # Get output schema configuration
        output_schema = self.config.get_output_schema(self.endpoint_name)
        if not output_schema or 'tables' not in output_schema:
            raise ValueError(f"No output_schema.tables configured for endpoint '{self.endpoint_name}'")
        
        # Fetch and parse data
        raw_data = await self._fetch_and_parse(params)
        
        # Transform to DataFrames based on output schema
        dataframes = self._transform_data(raw_data, output_schema['tables'])
        
        # Save to parquet files
        output_files = []
        for table_name, df in dataframes.items():
            filepath = self._save_dataframe(df, table_name, timestamp_suffix)
            output_files.append(filepath)
            
        logger.info(f"Generic processing complete for {self.endpoint_name}. Created {len(output_files)} files.")
        return output_files
        
    def _transform_data(self, raw_data: Dict[str, Any], table_configs: Dict[str, Any]) -> Dict[str, pl.DataFrame]:
        """
        Transform parsed XML data into DataFrames based on table configurations.
        
        Args:
            raw_data: Parsed XML data
            table_configs: Table configuration from output_schema.tables
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        api_schema = self.config.get_endpoint_schema(self.endpoint_name)
        dataframes = {}
        
        for table_name, table_config in table_configs.items():
            logger.debug(f"Processing table: {table_name}")
            
            source_path = table_config.get('source_path', '')
            fields = table_config.get('fields', [])
            foreign_key = table_config.get('foreign_key')
            
            # Extract data from source path
            table_data = self._extract_data_from_path(raw_data, source_path, foreign_key)
            
            # Create schema for this table
            table_schema = create_table_schema(fields, api_schema, self.config)
            
            # Create DataFrame
            if table_data:
                df = pl.DataFrame(table_data, schema=table_schema)
            else:
                df = pl.DataFrame(schema=table_schema)
                
            dataframes[table_name] = df
            
        logger.info(f"Transformed data: {', '.join([f'{len(df)} {name}' for name, df in dataframes.items()])}")
        return dataframes
        
    def _extract_data_from_path(
        self, 
        raw_data: Dict[str, Any], 
        source_path: str, 
        foreign_key: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract data from nested structure using dot notation path.
        
        Args:
            raw_data: Source data
            source_path: Dot notation path (e.g., "LeagueList" or "LeagueList.Country")
            foreign_key: Foreign key field to add from parent
            
        Returns:
            List of records for this table
        """
        if not source_path:
            return []
            
        path_parts = source_path.split('.')
        
        # Handle nested extraction first (e.g., LeagueList.Country, LeagueList.Cups)
        if len(path_parts) > 1:
            # We're extracting nested data (like countries from leagues)
            parent_data = raw_data
            for part in path_parts[:-1]:  # Navigate to parent
                parent_data = parent_data.get(part, [])
            
            extracted_records = []
            if isinstance(parent_data, list):
                for parent_record in parent_data:
                    nested_field = path_parts[-1]
                    nested_data = parent_record.get(nested_field)
                    
                    if nested_data is not None:
                        # Handle both single objects and arrays
                        if isinstance(nested_data, list):
                            # Array of nested objects (e.g., Cups)
                            for nested_record in nested_data:
                                # Add foreign key from parent if specified
                                if foreign_key and foreign_key in parent_record:
                                    nested_record[foreign_key] = parent_record[foreign_key]
                                extracted_records.append(nested_record)
                        elif isinstance(nested_data, dict):
                            # Single nested object (e.g., Country)
                            nested_record = nested_data.copy()
                            # Add foreign key from parent if specified
                            if foreign_key and foreign_key in parent_record:
                                nested_record[foreign_key] = parent_record[foreign_key]
                            extracted_records.append(nested_record)
            
            return extracted_records
        else:
            # Simple extraction from top level (e.g., just "LeagueList")
            current_data = raw_data.get(source_path, [])
            return current_data if isinstance(current_data, list) else []