# ht_api

A Python library for Hattrick API with YAML configuration and OAuth authentication.

## Features

- **YAML-based endpoint configuration** - Easy to add new endpoints
- **OAuth authentication** - Secure API access
- **Automatic XML parsing** - Structured data output
- **Multi-threading support** - Parallel API calls
- **Type-safe parsing** - Automatic data type conversion

## Installation

### From PyPI (when published)
```bash
pip install ht-chpp
```

### From source
```bash
git clone https://github.com/yourusername/ht-chpp.git
cd ht-chpp
pip install -e .
```

### Development installation
```bash
git clone https://github.com/yourusername/ht-chpp.git
cd ht_chpp
pip install -r requirements.txt
```

## Setup

1. **Copy environment template:**
```bash
cp .env.example .env
```

2. **Fill in your credentials in `.env`:**
```env
HT_CLIENT_ID=your_client_id_here
HT_CLIENT_SECRET=your_client_secret_here
HT_OAUTH_TOKEN=your_oauth_token_here
HT_OAUTH_TOKEN_SECRET=your_oauth_token_secret_here
```

## Usage

### Basic Usage

```python
from auth import get_ht_oauth
from ht_api import call_endpoint

# Get OAuth client and token
oauth, token = get_ht_oauth()

# Call API endpoint
leagues = call_endpoint('worlddetails', token=token, oauth=oauth)
print(f"Found {len(leagues)} leagues")
```

### Specific League

```python
# Get specific league
league = call_endpoint('worlddetails', leagueID=1, token=token, oauth=oauth)
print(f"League: {league[0]['LeagueName']}")
```

### Parallel API Calls

```python
from ht_api import call_endpoints_multithread

# Parallel calls
results = call_endpoints_multithread([
    ('worlddetails', {}),
    ('worlddetails', {'leagueID': 1})
], token=token, oauth=oauth)
```

### Data Structure

```python
leagues = call_endpoint('worlddetails', token=token, oauth=oauth)

# Access league data
first_league = leagues[0]
print(f"League: {first_league['LeagueName']}")
print(f"Country: {first_league['Country'][0]['CountryName']}")
print(f"Cups: {len(first_league['Cups'])} cups")
```

## Adding New Endpoints

1. **Add endpoint definition to `endpoints.yaml`:**

```yaml
achievements:
  version:
    "1.9":
      params:
        - userID
      fields:
        Achievement:
          - AchievementTypeID: int
          - AchievementText: str
          - CategoryID: int
      collections:
        Achievement:
          - AchievementTypeID: int
          - AchievementText: str
          - CategoryID: int
```

2. **Use the endpoint:**

```python
achievements = call_endpoint('achievements', userID=123, token=token, oauth=oauth)
```

## YAML Configuration Structure

```yaml
endpoint_name:
  version:
    "1.9":
      params:           # API parameters
        - param1
        - param2
      fields:           # Main object fields
        MainObject:
          - Field1: int
          - Field2: str
          - Collection: list
      collections:      # Collection definitions
        Collection:
          - ItemField1: int
          - ItemField2: str
```

## Data Types

- `int` - Integer values
- `float` - Float values (handles comma decimal separators)
- `str` - String values
- `bool` - Boolean values (for XML attributes)
- `list` - Collections of objects

## Error Handling

The library provides clear error messages:

```python
try:
    leagues = call_endpoint('worlddetails', token=token, oauth=oauth)
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"API error: {e}")
```


## License

MIT License