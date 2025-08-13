"""
Type conversion utilities for YAML to Polars schema mapping.
"""

from typing import Dict, Any, List
import polars as pl

try:
    from ..config import Config
except ImportError:
    from config import Config


def get_polars_type_mapping(config: Config) -> Dict[str, pl.DataType]:
    """
    Get type mapping from YAML definitions to Polars types.
    
    Args:
        config: Configuration instance with loaded definitions
        
    Returns:
        Dictionary mapping YAML type names to Polars DataType instances
    """
    definitions = config.get_definitions()
    yaml_types = definitions.get('types', {})
    
    # Map string type names to actual Polars types
    polars_type_map = {
        'Int64': pl.Int64,
        'Utf8': pl.Utf8,
        'Float64': pl.Float64,
        'Boolean': pl.Boolean,
        'Date': pl.Date,
        'Datetime': pl.Datetime,
    }
    
    # Convert YAML config to Polars types
    type_mapping = {}
    for yaml_type, polars_type_name in yaml_types.items():
        if polars_type_name in polars_type_map:
            type_mapping[yaml_type] = polars_type_map[polars_type_name]
        else:
            # Fallback to Utf8 for unknown types
            type_mapping[yaml_type] = pl.Utf8
            
    return type_mapping


def extract_field_types(yaml_schema: Dict[str, Any], config: Config) -> Dict[str, pl.DataType]:
    """
    Extract field types from YAML schema definition.
    
    Args:
        yaml_schema: Schema definition from YAML config
        config: Configuration instance
        
    Returns:
        Dictionary mapping field names to Polars types
    """
    type_mapping = get_polars_type_mapping(config)
    field_types = {}
    
    def _extract_from_fields(fields: List[Dict[str, Any]], prefix: str = ""):
        """Recursively extract field types from nested structure."""
        for field_def in fields:
            for field_name, field_config in field_def.items():
                full_field_name = f"{prefix}{field_name}" if prefix else field_name
                
                if isinstance(field_config, str):
                    # Simple type field
                    polars_type = type_mapping.get(field_config, pl.Utf8)
                    field_types[full_field_name] = polars_type
                elif isinstance(field_config, dict):
                    # Nested structure - recursively extract
                    nested_type = list(field_config.keys())[0]
                    nested_fields = field_config[nested_type]
                    _extract_from_fields(nested_fields, f"{field_name}.")
    
    # Extract from main schema structure
    main_object_type = list(yaml_schema.keys())[0]
    main_fields = yaml_schema[main_object_type]
    _extract_from_fields(main_fields)
    
    return field_types


def create_table_schema(field_names: List[str], yaml_schema: Dict[str, Any], config: Config) -> Dict[str, pl.DataType]:
    """
    Create Polars schema for specific table fields.
    
    Args:
        field_names: List of field names for this table
        yaml_schema: Full YAML schema definition
        config: Configuration instance
        
    Returns:
        Dictionary suitable for pl.DataFrame(schema=...)
    """
    all_field_types = extract_field_types(yaml_schema, config)
    type_mapping = get_polars_type_mapping(config)
    
    # Filter to only requested fields
    table_schema = {}
    for field_name in field_names:
        # Try exact match first
        if field_name in all_field_types:
            table_schema[field_name] = all_field_types[field_name]
        else:
            # Try to find field in nested paths (e.g., LeagueList.LeagueID -> LeagueID)
            found = False
            for full_name, field_type in all_field_types.items():
                if full_name.endswith(f".{field_name}") or full_name == field_name:
                    table_schema[field_name] = field_type
                    found = True
                    break
            
            if not found:
                # Check if this is a main-level field by looking at YAML directly
                main_type = list(yaml_schema.keys())[0]
                main_fields = yaml_schema[main_type]
                
                for field_def in main_fields:
                    if field_name in field_def:
                        yaml_type = field_def[field_name]
                        if isinstance(yaml_type, str):
                            table_schema[field_name] = type_mapping.get(yaml_type, pl.Utf8)
                            found = True
                            break
                
                if not found:
                    # Default to Utf8 for missing fields
                    table_schema[field_name] = pl.Utf8
            
    return table_schema