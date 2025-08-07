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

def get_latest_version(endpoint_name):
    """Gets the latest version for an endpoint from YAML configuration."""
    config = load_endpoints_config()
    
    if endpoint_name not in config:
        raise ValueError(f"Endpoint '{endpoint_name}' is not defined in YAML configuration")
    
    endpoint_config = config[endpoint_name]
    
    # Check if endpoint has version structure
    if 'version' in endpoint_config:
        # Get all available versions and return the highest
        versions = list(endpoint_config['version'].keys())
        # Convert to float for proper comparison (handle "1.9" vs "1.10")
        versions_float = [float(v) for v in versions]
        latest_version = str(max(versions_float))
        return latest_version
    else:
        # Old structure without versioning
        return "1.0"  # Default fallback

def parse_nested_structure(element, field_definitions):
    """Recursively parses nested structure according to field definitions."""
    parsed_obj = {}
    
    for field_def in field_definitions:
        for field_name, field_config in field_def.items():
            try:
                if isinstance(field_config, dict):
                    # Nested structure (e.g., LeagueList: { League: [...] })
                    nested_object_type = list(field_config.keys())[0]
                    nested_object_definitions = field_config[nested_object_type]
                    
                    # Check if there are multiple elements of this type directly under parent
                    # This handles cases like multiple <Team> elements under <HattrickData>
                    direct_elements = element.findall(field_name)
                    if len(direct_elements) > 1:
                        # Multiple elements - treat as collection
                        nested_objects = []
                        for nested_obj in direct_elements:
                            parsed_nested_obj = parse_nested_structure(nested_obj, nested_object_definitions)
                            nested_objects.append(parsed_nested_obj)
                        parsed_obj[field_name] = nested_objects
                    elif len(direct_elements) == 1:
                        # Single element - check if it has nested objects or is a simple object
                        nested_element = direct_elements[0]
                        if nested_element.find(nested_object_type) is not None:
                            # List of objects (e.g., LeagueList/League)
                            nested_objects = []
                            for nested_obj in element.findall(f'{field_name}/{nested_object_type}'):
                                parsed_nested_obj = parse_nested_structure(nested_obj, nested_object_definitions)
                                nested_objects.append(parsed_nested_obj)
                            parsed_obj[field_name] = nested_objects
                        else:
                            # Single object (like Country with attributes)
                            parsed_nested_obj = parse_nested_structure(nested_element, nested_object_definitions)
                            parsed_obj[field_name] = [parsed_nested_obj]
                    else:
                        parsed_obj[field_name] = []
                else:
                    # Simple field with type
                    field_type = field_config
                    
                    # Check if it's an attribute (starts with @)
                    if field_name.startswith('@'):
                        attr_name = field_name[1:]  # Remove @
                        parsed_obj[attr_name] = parse_attribute_value(element, attr_name, field_type)
                    else:
                        # Regular element - check if there are multiple elements
                        field_elements = element.findall(field_name)
                        if len(field_elements) > 1:
                            # Multiple elements - parse each one
                            parsed_values = []
                            for field_element in field_elements:
                                parsed_value = parse_element_value(field_element, field_type)
                                parsed_values.append(parsed_value)
                            parsed_obj[field_name] = parsed_values
                        elif len(field_elements) == 1:
                            # Single element
                            parsed_obj[field_name] = parse_element_value(field_elements[0], field_type)
                        else:
                            # Return default value based on type
                            if field_type == 'int':
                                parsed_obj[field_name] = 0
                            elif field_type == 'float':
                                parsed_obj[field_name] = 0.0
                            elif field_type == 'bool':
                                parsed_obj[field_name] = False
                            elif field_type == 'str':
                                parsed_obj[field_name] = ''
                            else:
                                parsed_obj[field_name] = ''
                                
            except Exception as e:
                print(f"Error parsing field {field_name}: {e}")
                # Return default value based on type
                if isinstance(field_config, dict):
                    parsed_obj[field_name] = []
                else:
                    field_type = field_config
                    if field_type == 'int':
                        parsed_obj[field_name] = 0
                    elif field_type == 'float':
                        parsed_obj[field_name] = 0.0
                    elif field_type == 'bool':
                        parsed_obj[field_name] = False
                    elif field_type == 'str':
                        parsed_obj[field_name] = ''
                    else:
                        parsed_obj[field_name] = ''
    
    return parsed_obj

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
    schema_config = endpoint_config['schema']
    
    # Get main object type (first in configuration, e.g., 'HattrickData')
    main_object_type = list(schema_config.keys())[0]
    main_fields = schema_config[main_object_type]
    
    # Parse the root element
    parsed_main_obj = parse_nested_structure(root, main_fields)
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
def call_endpoint(endpoint_name, version=None, token=None, oauth=None, **kwargs):
    """
    Calls API endpoint and returns parsed data.
    Args:
        endpoint_name (str): Endpoint name from YAML configuration
        version (str, optional): API version (defaults to latest available version)
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
    
    # If no version specified, use the latest available version
    if version is None:
        version = get_latest_version(endpoint_name)
    
    # Check if requested version exists
    if 'version' in endpoint_config:
        # Version structure
        if version not in endpoint_config.get('version', {}):
            raise ValueError(f"Version '{version}' is not supported for endpoint '{endpoint_name}'")
        version_config = endpoint_config['version'][version]
    else:
        # Old structure (without versioning)
        if version not in endpoint_config:
            raise ValueError(f"Version '{version}' is not supported for endpoint '{endpoint_name}'")
        version_config = endpoint_config[version]
    
    # Build parameters
    params = {
        'version': version
    }
    
    # Add optional parameters
    for param in version_config.get('parameters', []):
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
def call_endpoints_multithread(endpoints_with_params, token=None, oauth=None, max_workers=50):
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
