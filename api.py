import requests
import yaml
import xml.etree.ElementTree as ET
import os
from concurrent.futures import ThreadPoolExecutor

def safe_float(value):
    """Safely converts string to float, handles comma decimal separators."""
    if isinstance(value, str):
        return float(value.replace(',', '.'))
    return float(value)

def parse_element_value(element, field_type):
    """Parses XML element value by type."""
    if element is not None and element.text:
        if field_type == 'int':
            return int(element.text)
        elif field_type == 'float':
            return safe_float(element.text)
        elif field_type == 'str':
            return element.text
        elif field_type == 'bool':
            return element.text.lower() == 'true'
        else:
            return element.text
    
    # Return default values
    if field_type == 'int':
        return 0
    elif field_type == 'float':
        return 0.0
    elif field_type == 'bool':
        return False
    elif field_type == 'list':
        return []
    else:
        return ''

def parse_attribute_value(element, attr_name, attr_type):
    """Parses XML attribute value by type."""
    attr_value = element.get(attr_name)
    if attr_value is not None:
        if attr_type == 'bool':
            return attr_value.lower() == 'true'
        elif attr_type == 'int':
            return int(attr_value)
        elif attr_type == 'float':
            return safe_float(attr_value)
        elif attr_type == 'str':
            return attr_value
        else:
            return attr_value
    
    # Return default values
    if attr_type == 'int':
        return 0
    elif attr_type == 'float':
        return 0.0
    elif attr_type == 'bool':
        return False
    elif attr_type == 'list':
        return []
    else:
        return ''

# Load YAML endpoint definitions
def load_endpoints_config():
    """Loads YAML endpoint configuration."""
    config_path = os.path.join(os.path.dirname(__file__), 'endpoints.yaml')
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# Parse single object by definition
def parse_object_by_definition(obj_element, field_definitions):
    """Parses single object by field definitions."""
    parsed_obj = {}
    
    for field_def in field_definitions:
        for field_name, field_type in field_def.items():
            try:
                # Try to find as element
                field_element = obj_element.find(field_name)
                if field_element is not None and field_element.text:
                    parsed_obj[field_name] = parse_element_value(field_element, field_type)
                else:
                    # Try to find as attribute
                    parsed_obj[field_name] = parse_attribute_value(obj_element, field_name, field_type)
            except Exception as e:
                print(f"Error parsing field {field_name}: {e}")
                # Return default value based on type
                if field_type == 'int':
                    parsed_obj[field_name] = 0
                elif field_type == 'float':
                    parsed_obj[field_name] = 0.0
                elif field_type == 'bool':
                    parsed_obj[field_name] = False
                elif field_type == 'list':
                    parsed_obj[field_name] = []
                else:
                    parsed_obj[field_name] = ''
    
    return parsed_obj

# Parse Country object (special case)
def parse_country_collection(league_element, country_definitions):
    """Parses Country collection (special case - Country is directly in League)."""
    country_obj = league_element.find('Country')
    if country_obj is not None:
        return [parse_object_by_definition(country_obj, country_definitions)]
    return []

# Parse standard collection
def parse_standard_collection(parent_element, collection_name, object_definitions):
    """Parses standard collection of objects (e.g., Cups)."""
    list_object_type = collection_name.rstrip('s')  # Cups -> Cup
    nested_objects = []
    
    for nested_obj in parent_element.findall(f'.//{collection_name}/{list_object_type}'):
        parsed_obj = parse_object_by_definition(nested_obj, object_definitions)
        nested_objects.append(parsed_obj)
    
    return nested_objects

# Parse collection by type
def parse_collection(parent_element, collection_name, collection_definitions):
    """Parses collection by its type."""
    if collection_name == 'Country':
        return parse_country_collection(parent_element, collection_definitions)
    else:
        return parse_standard_collection(parent_element, collection_name, collection_definitions)

# Main XML parsing function
def parse_xml_response(xml_text, endpoint_config):
    """
    Parses XML response according to YAML endpoint configuration.
    Args:
        xml_text (str): XML response from API
        endpoint_config (dict): Endpoint configuration from YAML
    Returns:
        list: List of parsed objects with nested objects
    """
    # Decode XML
    xml = xml_text.encode("latin-1").decode("utf-8")
    root = ET.fromstring(xml)
    
    results = []
    fields_config = endpoint_config['fields']
    collections_config = endpoint_config.get('collections', {})
    
    # Find main object (first in configuration, e.g., 'League')
    main_object_type = list(fields_config.keys())[0]
    main_fields = fields_config[main_object_type]
    
    # Find all main objects in XML
    for main_obj in root.findall(f'.//{main_object_type}'):
        parsed_main_obj = {}
        
        # Parse main fields
        for field_def in main_fields:
            for field_name, field_type in field_def.items():
                try:
                    if field_type == 'list':
                        # Parse collection
                        if field_name in collections_config:
                            collection_definitions = collections_config[field_name]
                        else:
                            raise ValueError(f"Collection '{field_name}' is not defined in YAML configuration for endpoint")
                        
                        parsed_main_obj[field_name] = parse_collection(
                            main_obj, field_name, collection_definitions
                        )
                    else:
                        # Parse simple field
                        field_element = main_obj.find(field_name)
                        parsed_main_obj[field_name] = parse_element_value(field_element, field_type)
                        
                except Exception as e:
                    print(f"Error parsing field {field_name}: {e}")
                    # Return default value based on type
                    if field_type == 'int':
                        parsed_main_obj[field_name] = 0
                    elif field_type == 'float':
                        parsed_main_obj[field_name] = 0.0
                    elif field_type == 'bool':
                        parsed_main_obj[field_name] = False
                    elif field_type == 'list':
                        parsed_main_obj[field_name] = []
                    else:
                        parsed_main_obj[field_name] = ''
        
        results.append(parsed_main_obj)
    
    return results

# General function for calling Hattrick API
def call_ht_api(endpoint, params=None, token=None, oauth=None):
    """
    General function for calling Hattrick API endpoint.
    Args:
        endpoint (str): Endpoint name (e.g., 'worlddetails')
        params (dict): Query parameters
        token (dict): Dictionary with oauth_token and oauth_token_secret
        oauth (OAuth): OAuth client for authenticated calls
    Returns:
        response (requests.Response or OAuth response): API response
    """
    # Build parameters
    api_params = {
        'file': endpoint
    }
    if params:
        api_params.update(params)
    
    # If we have OAuth client and token, use authenticated call
    if oauth and token:
        try:
            response = oauth.hattrick.get('', params=api_params, token=token)
            return response
        except Exception as e:
            print(f"OAuth call failed: {e}")
            # Fallback to unauthenticated call
    
    # Unauthenticated call (fallback)
    url = "https://chpp.hattrick.org/chppxml.ashx"
    headers = {
        'User-Agent': 'ht_api/1.0'
    }
    response = requests.get(url, params=api_params, headers=headers)
    return response

# Main function for calling endpoint with parsing
def call_endpoint(endpoint_name, version="1.9", token=None, oauth=None, **kwargs):
    """
    Calls API endpoint and returns parsed data.
    Args:
        endpoint_name (str): Endpoint name from YAML configuration
        version (str): API version (default "1.9")
        token (dict): OAuth token for authentication
        oauth (OAuth): OAuth client for authentication
        **kwargs: Parameters for API call
    Returns:
        list: Parsed data
    """
    config = load_endpoints_config()
    
    if endpoint_name not in config:
        raise ValueError(f"Endpoint '{endpoint_name}' is not defined in YAML configuration")
    
    endpoint_config = config[endpoint_name]
    
    # Check if requested version exists
    if 'version' in endpoint_config:
        # Old structure (without versioning)
        if version not in endpoint_config.get('version', {}):
            raise ValueError(f"Version '{version}' is not supported for endpoint '{endpoint_name}'")
        version_config = endpoint_config['version'][version]
    else:
        # New structure (with versioning)
        if version not in endpoint_config:
            raise ValueError(f"Version '{version}' is not supported for endpoint '{endpoint_name}'")
        version_config = endpoint_config[version]
    
    # Build parameters
    params = {
        'version': version
    }
    
    # Add optional parameters
    for param in version_config.get('params', []):
        if param in kwargs:
            params[param] = kwargs[param]
    
    # Call API
    response = call_ht_api(endpoint_name, params, token, oauth)
    
    # Check response
    if hasattr(response, 'status_code') and response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}")
    
    # Parse response
    return parse_xml_response(response.text, version_config)

# Multi-thread wrapper for multiple endpoints
def call_endpoints_multithread(endpoints_with_params, token=None, oauth=None, max_workers=5):
    """
    Parallel calls to multiple API endpoints.
    Args:
        endpoints_with_params (list): List of (endpoint_name, params_dict) tuples
        token (dict): OAuth token for authentication
        oauth (OAuth): OAuth client for authentication
        max_workers (int): Number of threads
    Returns:
        list: Call results
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for endpoint_name, params in endpoints_with_params:
            future = executor.submit(call_endpoint, endpoint_name, token=token, oauth=oauth, **params)
            futures.append(future)
        
        results = [f.result() for f in futures]
    return results
