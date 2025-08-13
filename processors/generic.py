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
            parent_fields = table_config.get('parent_fields', [])
            
            # Extract data from source path
            table_data = self._extract_data_from_path(raw_data, source_path, foreign_key, parent_fields)
            logger.debug(f"Extracted {len(table_data)} records from {source_path}")
            if table_data and len(table_data) > 0:
                logger.debug(f"First record keys: {list(table_data[0].keys()) if isinstance(table_data[0], dict) else 'Not dict'}")
            
            # Flatten nested objects in extracted data
            if table_data:
                table_data = self._flatten_nested_objects(table_data, api_schema, source_path)
            
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
        foreign_key: Optional[str] = None,
        parent_fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract data from nested structure using dot notation path.
        
        Args:
            raw_data: Source data
            source_path: Dot notation path (e.g., "LeagueList" or "LeagueList.Country")
            foreign_key: Foreign key field to add from parent
            parent_fields: Fields to inject from top-level data into extracted records
            
        Returns:
            List of records for this table
        """
        if not source_path:
            return []
            
        path_parts = source_path.split('.')
        parent_fields = parent_fields or []
        
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
            if not isinstance(current_data, list):
                return []
            
            # Inject parent fields if specified
            if parent_fields:
                extracted_records = []
                for record in current_data:
                    record_copy = record.copy()
                    # Add parent fields from top-level raw_data
                    for field in parent_fields:
                        if field in raw_data:
                            record_copy[field] = raw_data[field]
                    extracted_records.append(record_copy)
                return extracted_records
            else:
                return current_data
                
    def _flatten_nested_objects(
        self,
        table_data: List[Dict[str, Any]], 
        api_schema: Dict[str, Any],
        source_path: str
    ) -> List[Dict[str, Any]]:
        """
        Flatten nested objects in extracted table data based on API schema.
        
        Converts nested structures like:
        {'Language': {'Language': {'LanguageId': 1, 'LanguageName': 'English'}}}
        
        To flat structure:
        {'LanguageId': 1, 'LanguageName': 'English'}
        
        Args:
            table_data: Extracted table records
            api_schema: API schema definition
            source_path: Source path used for extraction
            
        Returns:
            Flattened table records
        """
        if not table_data or not api_schema:
            return table_data
            
        # Find the schema definition for our source path
        schema_def = self._find_schema_definition(api_schema, source_path)
        logger.debug(f"Schema def for {source_path}: {schema_def}")
        if not schema_def:
            logger.debug(f"No schema definition found for {source_path}")
            return table_data
            
        flattened_data = []
        for record in table_data:
            logger.debug(f"Original record keys: {list(record.keys()) if isinstance(record, dict) else 'Not dict'}")
            flattened_record = self._flatten_record(record, schema_def)
            logger.debug(f"Flattened record keys: {list(flattened_record.keys()) if isinstance(flattened_record, dict) else 'Not dict'}")
            flattened_data.append(flattened_record)
            
        return flattened_data
        
    def _find_schema_definition(self, api_schema: Dict[str, Any], source_path: str) -> Dict[str, Any]:
        """Find schema definition for given source path."""
        if not source_path:
            return api_schema
            
        # API schema is nested under HattrickData, so start there
        if 'HattrickData' not in api_schema:
            return {}
            
        current_schema = api_schema['HattrickData']
        path_parts = source_path.split('.')
        
        for part in path_parts:
            logger.debug(f"Looking for part '{part}' in schema type: {type(current_schema)}")
            if isinstance(current_schema, list):
                # Handle list of field definitions
                found = False
                for item in current_schema:
                    if isinstance(item, dict) and part in item:
                        current_schema = item[part]
                        logger.debug(f"Found {part} in list, new schema: {current_schema}")
                        found = True
                        break
                if not found:
                    logger.debug(f"Part '{part}' not found in list")
                    return {}
            elif isinstance(current_schema, dict):
                if part in current_schema:
                    current_schema = current_schema[part]
                    logger.debug(f"Found {part} in dict, new schema: {current_schema}")
                else:
                    # Try to find the part in nested type definitions
                    # This handles double nesting like Manager: Manager: Teams:
                    found_nested = False
                    for key, value in current_schema.items():
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict) and part in item:
                                    current_schema = item[part]
                                    logger.debug(f"Found {part} in nested list under {key}, new schema: {current_schema}")
                                    found_nested = True
                                    break
                            if found_nested:
                                break
                    if not found_nested:
                        logger.debug(f"Part '{part}' not found in dict")
                        return {}
            else:
                logger.debug(f"Schema is not dict or list, type: {type(current_schema)}")
                return {}
                
        return current_schema if isinstance(current_schema, dict) else {}
        
    def _flatten_record(self, record: Dict[str, Any], schema_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a single record based on schema definition.
        
        Handles nested objects like Language.Language, Country.Country, etc.
        """
        if not isinstance(record, dict):
            return record
            
        flattened = {}
        
        # Schema def has structure like {'Manager': [...field definitions...]}
        # We need to get to the field definitions
        field_definitions = None
        if isinstance(schema_def, dict):
            for key, value in schema_def.items():
                if isinstance(value, list):
                    field_definitions = value
                    break
                    
        if not field_definitions:
            return record
            
        # Process each field in the record
        for field_name, field_value in record.items():
            logger.debug(f"Processing field {field_name}, type: {type(field_value)}")
            if isinstance(field_value, list) and len(field_value) == 1 and isinstance(field_value[0], dict):
                # This is a single-element list (already flattened by parser)
                # Extract the first element and flatten it
                nested_obj = field_value[0]
                for nested_key, nested_value in nested_obj.items():
                    logger.debug(f"Flattening list {field_name}[0].{nested_key} = {nested_value}")
                    # Check if this nested value is also a single-element list that needs flattening
                    if isinstance(nested_value, list) and len(nested_value) == 1 and isinstance(nested_value[0], dict):
                        # This is a nested single-element list (e.g., YouthLeague within YouthTeam)
                        deep_nested_obj = nested_value[0]
                        for deep_key, deep_value in deep_nested_obj.items():
                            logger.debug(f"Deep flattening {field_name}[0].{nested_key}[0].{deep_key} = {deep_value}")
                            flattened[deep_key] = deep_value
                    else:
                        flattened[nested_key] = nested_value
            elif isinstance(field_value, dict):
                # Handle regular dict objects
                field_def = self._find_field_definition(field_definitions, field_name)
                if field_def and isinstance(field_def, dict) and field_name in field_def:
                    # This is a nested object like Language.Language
                    nested_obj = field_value.get(field_name, {})
                    if isinstance(nested_obj, dict):
                        # Flatten the nested object directly
                        for nested_key, nested_value in nested_obj.items():
                            logger.debug(f"Flattening dict {field_name}.{field_name}.{nested_key} = {nested_value}")
                            flattened[nested_key] = nested_value
                    else:
                        flattened[field_name] = field_value
                else:
                    # Regular nested object - copy as is for now
                    flattened[field_name] = field_value
            else:
                # Regular field - copy as is
                flattened[field_name] = field_value
                
        return flattened
        
    def _find_field_definition(self, field_definitions: List[Dict], field_name: str) -> Dict[str, Any]:
        """Find field definition in list of field definitions."""
        for field_def in field_definitions:
            if isinstance(field_def, dict) and field_name in field_def:
                return field_def
        return {}