# ht_chpp

A Python library for Hattrick API with YAML configuration and OAuth authentication.

## Features

- **YAML-based endpoint configuration** - Easy to add new endpoints with nested structure
- **OAuth authentication** - Secure API access
- **Automatic XML parsing** - Structured data output with metadata
- **Multi-threading support** - Parallel API calls (up to 50 concurrent requests)
- **Type-safe parsing** - Automatic data type conversion
- **Unified response structure** - All endpoints return HattrickData with metadata
- **Nested schema definition** - XML structure directly mapped in YAML

## Available Endpoints

- **achievements** (v1.2) - User achievements and statistics
- **leaguedetails** (v1.6) - League table and team statistics
- **leaguelevels** (v1.0) - League level structure
- **managercompendium** (v1.5) - Manager profile and team details
- **worlddetails** (v1.9) - League and country information

## Installation

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

### Authentication

All API calls require OAuth authentication. First, get your OAuth client and token:

```python
from ht_chpp import get_ht_oauth

# Get OAuth client and token
oauth, token = get_ht_oauth()
```

**Note:** Make sure your `.env` file is properly configured with your Hattrick API credentials.

### Basic Usage

```python
from ht_chpp import get_ht_oauth, call_endpoint

# Get OAuth client and token
oauth, token = get_ht_oauth()

# Call API endpoint (uses latest version automatically)
leagues = call_endpoint('worlddetails', token=token, oauth=oauth)
print(f"Found {len(leagues[0]['LeagueList'])} leagues")

# Or specify a specific version
leagues_v1_9 = call_endpoint('worlddetails', version="1.9", token=token, oauth=oauth)
```

### Specific League

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

# Get specific league (uses latest version automatically)
league = call_endpoint('worlddetails', leagueID=1, token=token, oauth=oauth)
print(f"League: {league[0]['LeagueList'][0]['LeagueName']}")

# Get league levels for Sweden
league_levels = call_endpoint('leaguelevels', LeagueID=1, token=token, oauth=oauth)
print(f"League has {len(league_levels[0]['LeagueLevelList'])} levels")

# Get details for specific league unit
league_details = call_endpoint('leaguedetails', leagueLevelUnitID=11323, token=token, oauth=oauth)
print(f"League: {league_details[0]['LeagueName']}")
print(f"Teams: {len(league_details[0]['Team'])} teams")

### Manager Information

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

# Get manager compendium (uses latest version automatically)
manager = call_endpoint('managercompendium', token=token, oauth=oauth)
print(f"Manager: {manager[0]['Manager'][0]['Loginname']}")
print(f"Team: {manager[0]['Manager'][0]['Teams'][0]['TeamName']}")
print(f"Country: {manager[0]['Manager'][0]['Country'][0]['CountryName']}")

# Get manager compendium for specific user
manager = call_endpoint('managercompendium', userId=123456, token=token, oauth=oauth)
```

### User Achievements

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

# Get achievements for current user (uses latest version automatically)
achievements = call_endpoint('achievements', token=token, oauth=oauth)
print(f"Total achievements: {len(achievements[0]['AchievementList'])}")
print(f"Max points: {achievements[0]['MaxPoints']}")

# Get achievements for specific user (userID parameter is optional)
achievements = call_endpoint('achievements', userID=123456, token=token, oauth=oauth)

# Access achievement details
for achievement in achievements[0]['AchievementList']:
    print(f"Achievement: {achievement['AchievementTitle']}")
    print(f"Points: {achievement['Points']}")
    print(f"Category: {achievement['CategoryID']}")
    print(f"Date: {achievement['EventDate']}")

# Get achievements for multiple users in parallel
results = call_endpoint_multithread('achievements', 
                                   userID=[123456, 789012, 345678], 
                                   token=token, oauth=oauth)

for i, result in enumerate(results):
    user_id = [123456, 789012, 345678][i]
    achievement_count = len(result[0]['AchievementList'])
    max_points = result[0]['MaxPoints']
    print(f"User {user_id}: {achievement_count} achievements, {max_points} max points")

### Parallel API Calls

```python
from ht_chpp import call_endpoint_multithread

# Get OAuth client and token
oauth, token = get_ht_oauth()

# Parallel calls for same endpoint with different IDs
# Example: Get manager data for multiple users
results = call_endpoint_multithread('managercompendium', 
                                   userId=[123, 456, 789, 101112], 
                                   token=token, oauth=oauth)

# Example: Get league details for multiple league units
results = call_endpoint_multithread('leaguedetails',
                                   leagueLevelUnitID=[11323, 11324, 11325, 11326],
                                   token=token, oauth=oauth)

# Example: Get world details for multiple leagues
results = call_endpoint_multithread('worlddetails',
                                   leagueID=[1, 2, 3, 4],
                                   token=token, oauth=oauth)

# Example: Get achievements for multiple users
results = call_endpoint_multithread('achievements',
                                   userID=[123456, 789012, 345678],
                                   token=token, oauth=oauth)

# Custom number of workers
results = call_endpoint_multithread('managercompendium', 
                                   userId=[123, 456, 789], 
                                   token=token, oauth=oauth, 
                                   max_workers=10)
```

### Data Structure

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

leagues = call_endpoint('worlddetails', token=token, oauth=oauth)

# Access metadata
hattrick_data = leagues[0]
print(f"API Version: {hattrick_data['Version']}")
print(f"Fetched: {hattrick_data['FetchedDate']}")

# Access league data
first_league = hattrick_data['LeagueList'][0]
print(f"League: {first_league['LeagueName']}")
print(f"Country: {first_league['Country'][0]['CountryName']}")
print(f"Cups: {len(first_league['Cups'])} cups")

### Parallel Data Structure

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

# Parallel calls return list of results in same order as IDs list
user_ids = [123, 456, 789]
results = call_endpoint_multithread('managercompendium', userId=user_ids, token=token, oauth=oauth)

# Each result has same structure as single call
for i, result in enumerate(results):
    manager_data = result[0]  # Same as single call result
    print(f"User {user_ids[i]}: {manager_data['Manager'][0]['Loginname']}")
```

## Adding New Endpoints

1. **Add endpoint definition to `endpoints.yaml`:**

```yaml
achievements:
  version:
    "1.9":
      parameters:
        - userID
      schema:
        HattrickData:
          - FileName: str
          - Version: str
          - UserID: int
          - FetchedDate: str
          - AchievementList:
              Achievement:
                - AchievementTypeID: int
                - AchievementText: str
                - CategoryID: int
```

**Note:** The schema structure directly mirrors the XML structure. Nested objects are automatically parsed as lists when multiple elements exist.

2. **Use the endpoint:**

```python
# Get OAuth client and token
oauth, token = get_ht_oauth()

achievements = call_endpoint('achievements', userID=123, token=token, oauth=oauth)
```

## YAML Configuration Structure

```yaml
endpoint_name:
  version:
    "1.9":
      parameters:       # API parameters
        - param1
        - param2
      schema:           # Data structure (HattrickData)
        HattrickData:
          - FileName: str
          - Version: str
          - UserID: int
          - FetchedDate: str
          - Collection:
              Item:
                - ItemField1: int
                - ItemField2: str
```

### Nested Structure Benefits

- **Intuitive mapping** - YAML structure matches XML structure
- **No separate collections** - Everything defined inline
- **Flexible nesting** - Support for complex XML hierarchies
- **Cleaner code** - No hardcoded collection references

## Data Types

- `int` - Integer values
- `float` - Float values (handles comma decimal separators)
- `str` - String values
- `bool` - Boolean values (for XML attributes)
- Nested objects - Automatically parsed as lists when multiple elements exist

## Response Structure

All endpoints return data wrapped in `HattrickData` with metadata:

```python
[
  {
    "FileName": "worlddetails.xml",
    "Version": "1.9", 
    "UserID": 4351891,
    "FetchedDate": "2025-08-07 16:23:51",
    "LeagueList": [...],  # Actual data
    # ... other fields
  }
]
```

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

### Parameter Validation

The multithread function validates parameters against the YAML configuration:

```python
try:
    # This will raise an error - 'invalidParam' is not a valid parameter
    results = call_endpoint_multithread('managercompendium', 
                                       invalidParam=[123, 456], 
                                       token=token, oauth=oauth)
except ValueError as e:
    print(f"Parameter error: {e}")
    # Output: Parameter 'invalidParam' is not valid for endpoint 'managercompendium'. 
    # Valid parameters: ['userId']
```


## License

MIT License