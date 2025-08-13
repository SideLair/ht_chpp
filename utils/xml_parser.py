"""
Generic XML parsing utilities for Hattrick API responses.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Union
import logging

logger = logging.getLogger(__name__)


def safe_cast(value: str, target_type: str) -> Any:
    """Safely cast string value to target type."""
    if not value:
        return None
        
    try:
        if target_type == 'int':
            return int(value)
        elif target_type == 'float':
            return float(value.replace(',', '.'))
        elif target_type == 'bool':
            return value.lower() == 'true'
        elif target_type == 'str':
            return value
        else:
            return value
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to cast '{value}' to {target_type}: {e}")
        return None


def parse_element(element: ET.Element, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse XML element according to schema definition.
    
    Args:
        element: XML element to parse
        schema: Schema definition from YAML config
        
    Returns:
        Parsed data dictionary
    """
    result = {}
    
    for field_def in schema:
        for field_name, field_config in field_def.items():
            if isinstance(field_config, dict):
                # Nested structure
                nested_type = list(field_config.keys())[0]
                nested_schema = field_config[nested_type]
                
                # Find matching elements
                child_elements = element.findall(field_name)
                if len(child_elements) > 1:
                    # Multiple elements - create list
                    result[field_name] = [
                        parse_element(child, nested_schema) 
                        for child in child_elements
                    ]
                elif len(child_elements) == 1:
                    # Single element - check if it contains nested objects
                    child = child_elements[0]
                    nested_elements = child.findall(nested_type)
                    
                    if nested_elements:
                        # Contains nested objects
                        result[field_name] = [
                            parse_element(nested, nested_schema)
                            for nested in nested_elements
                        ]
                    else:
                        # Single nested object
                        result[field_name] = [parse_element(child, nested_schema)]
                else:
                    result[field_name] = []
                    
            else:
                # Simple field
                field_type = field_config
                
                if field_name.startswith('@'):
                    # Attribute
                    attr_name = field_name[1:]
                    value = element.get(attr_name)
                else:
                    # Element
                    child = element.find(field_name)
                    value = child.text if child is not None else None
                    
                result[field_name.lstrip('@')] = safe_cast(value, field_type)
                
    return result


def parse_xml_response(xml_text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse complete XML response according to schema.
    
    Args:
        xml_text: Raw XML response
        schema: Schema definition from config
        
    Returns:
        Parsed data dictionary
    """
    # Decode XML properly
    xml_content = xml_text.encode("latin-1").decode("utf-8")
    root = ET.fromstring(xml_content)
    
    # Get main schema structure
    main_object_type = list(schema.keys())[0]
    main_schema = schema[main_object_type]
    
    return parse_element(root, main_schema)