# HT CHPP

A modern Python library for Hattrick API data processing with clean architecture, generic processors, and parquet output.

## Features

- **🔧 Generic Processor System** - Single processor handles all endpoints via YAML config
- **📋 YAML-driven Configuration** - Easy endpoint definitions with input/output schemas
- **🔐 OAuth Authentication** - Secure Hattrick API access
- **⚡ Async/Await Support** - Fast, non-blocking API calls
- **📊 Parquet Output** - Type-safe data export with Polars
- **🎯 CLI Interface** - Simple command-line processing
- **🧪 Type Safety** - Pydantic validation + Polars schemas

## Quick Start

### Installation
```bash
git clone <your-repo>
cd ht_chpp
pip install -r requirements.txt
```

### Setup Authentication
Create `.env` file with your Hattrick API credentials:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Process Data
```bash
# List available endpoints
python cli.py list-endpoints

# Process worlddetails for all leagues
python cli.py process worlddetails

# Process worlddetails for Sweden (LeagueID=1)
python cli.py process worlddetails --league-id 1

# Show endpoint schema
python cli.py schema worlddetails
```

## Architecture

```
├── cli.py              # Command-line interface
├── client.py           # HTTP client with OAuth
├── config.py           # YAML configuration loader
├── processors/         # Data processors
│   ├── base.py         # Abstract base processor
│   └── generic.py      # Generic YAML-driven processor
├── utils/
│   ├── types.py        # Type conversion utilities
│   └── xml_parser.py   # XML parsing utilities
└── endpoints.yaml      # Endpoint configurations
```

## Available Endpoints

Current endpoints with output schema:

- **worlddetails** - League/country information → `leagues.parquet`, `countries.parquet`, `cups.parquet`

Other endpoints (API access only):
- **achievements** - User achievements
- **leaguedetails** - League tables  
- **leaguelevels** - League structures
- **managercompendium** - Manager profiles

## Usage Examples

### CLI Usage - Complete Workflow

```bash
# 1. Setup authentication 
cp .env.example .env
# Edit .env with your Hattrick API credentials

# 2. List available endpoints
python cli.py list-endpoints

# 3. Check endpoint schema
python cli.py schema worlddetails

# 4. Process all leagues to parquet files
python cli.py process worlddetails

# 5. Process specific league (Sweden)
python cli.py process worlddetails --league-id 1

# 6. View generated files
ls -la data/
# Output: leagues_20250813_143022.parquet, countries_20250813_143022.parquet, etc.
```

### Python API - Complete Example

```python
import asyncio
import polars as pl
from client import HTTPClient
from config import Config
from processors.generic import GenericProcessor

async def main():
    # 1. Authentication - load from .env file
    client = HTTPClient.from_env()
    config = Config()
    
    # 2. Create processor for worlddetails endpoint
    processor = GenericProcessor('worlddetails', client=client, config=config)
    
    # 3. Process data to parquet files
    async with client:
        output_files = await processor.process_to_parquet(
            params={'leagueID': 1},  # Sweden
            timestamp_suffix=True
        )
    
    print(f"✅ Created {len(output_files)} files:")
    for filepath in output_files:
        print(f"   📄 {filepath}")
    
    # 4. Load and analyze data
    for filepath in output_files:
        if filepath.exists():
            table_name = filepath.stem.split('_')[0]  # Extract table name
            df = pl.read_parquet(filepath)
            
            print(f"\n🔍 {table_name.upper()} TABLE:")
            print(f"   Rows: {len(df)}")
            print(f"   Columns: {df.columns}")
            
            if len(df) > 0:
                print(f"   Data preview:")
                print(df.head(3))
            else:
                print("   (Empty table)")

# Run the async function
if __name__ == "__main__":
    asyncio.run(main())
```

### Expected Output

```
✅ Created 3 files:
   📄 data/leagues_20250813_143022.parquet
   📄 data/countries_20250813_143022.parquet  
   📄 data/cups_20250813_143022.parquet

🔍 LEAGUES TABLE:
   Rows: 1
   Columns: ['LeagueID', 'LeagueName', 'Season', 'ActiveTeams', ...]
   Data preview:
   ┌───────────┬─────────────┬────────┬─────────────┐
   │ LeagueID  ┆ LeagueName  ┆ Season ┆ ActiveTeams │
   │ ---       ┆ ---         ┆ ---    ┆ ---         │
   │ i64       ┆ str         ┆ i64    ┆ i64         │
   ╞═══════════╪═════════════╪════════╪═════════════╡
   │ 1         ┆ Sweden      ┆ 89     ┆ 7616        │
   └───────────┴─────────────┴────────┴─────────────┘

🔍 COUNTRIES TABLE:
   Rows: 1
   Columns: ['LeagueID', 'CountryID', 'CountryName', 'CurrencyName', ...]
   Data preview:
   ┌───────────┬───────────┬─────────────┬──────────────┐
   │ LeagueID  ┆ CountryID ┆ CountryName ┆ CurrencyName │
   │ ---       ┆ ---       ┆ ---         ┆ ---          │
   │ i64       ┆ i64       ┆ str         ┆ str          │
   ╞═══════════╪═══════════╪═════════════╪══════════════╡
   │ 1         ┆ 1         ┆ Sweden      ┆ SEK          │
   └───────────┴───────────┴─────────────┴──────────────┘
```

### Data Analysis Workflow

```python
import polars as pl

# Load generated parquet files
leagues_df = pl.read_parquet("data/leagues_20250813_143022.parquet")
countries_df = pl.read_parquet("data/countries_20250813_143022.parquet")

# Basic data exploration
print(f"Total leagues: {len(leagues_df)}")
print(f"Schema: {dict(leagues_df.schema)}")

# Data analysis with Polars
top_leagues = (leagues_df
    .select(['LeagueName', 'ActiveTeams', 'ActiveUsers'])
    .sort('ActiveTeams', descending=True)
    .head(10)
)
print(top_leagues)

# Join with countries data
league_countries = (leagues_df
    .join(countries_df, on="LeagueID")
    .select(['LeagueName', 'CountryName', 'ActiveTeams', 'CurrencyName'])
)
print(league_countries)

# Export to different formats
league_countries.write_csv("analysis/league_summary.csv")
league_countries.write_json("analysis/league_summary.json")
```

## CLI Commands

```bash
# Process any endpoint with output_schema
python cli.py process <endpoint> [options]

# Available options:
--league-id INT                 # For worlddetails
--user-id INT                   # For achievements, managercompendium  
--league-level-unit-id INT      # For leaguedetails
--output-dir PATH               # Output directory (default: ./data)
--no-timestamp                  # Disable timestamp in filenames

# Utility commands
python cli.py list-endpoints    # Show available endpoints
python cli.py schema <endpoint> # Show endpoint schema
```

## Adding New Endpoints

1. **Add configuration to `endpoints.yaml`:**

```yaml
myendpoint:
  version:
    "1.0":
      parameters:
        - myParam
      api_schema:
        HattrickData:
          - FileName: str
          - Version: str
          - MyData:
              Item:
                - ItemID: int
                - ItemName: str
      output_schema:
        tables:
          items:
            source_path: "MyData.Item"
            fields:
              - ItemID
              - ItemName
```

2. **Use via CLI:**
```bash
python cli.py process myendpoint --my-param 123
```

## YAML Configuration

### Structure
```yaml
endpoint_name:
  version:
    "1.0":
      parameters: [param1, param2]    # API parameters
      api_schema: {...}               # XML input schema
      output_schema:                  # Parquet output config
        tables:
          table_name:
            source_path: "Data.Path"  # Dot notation path
            fields: [field1, field2]  # Field mapping
```

### Type Definitions
```yaml
definitions:
  types:
    int: "pl.Int64"
    str: "pl.Utf8" 
    float: "pl.Float64"
```

## Requirements

- Python 3.8+
- aiohttp (async HTTP)
- authlib (OAuth)
- polars (data processing)
- pydantic (validation)
- click (CLI)
- pyyaml (config)

## License

MIT License - see LICENSE file