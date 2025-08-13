#!/usr/bin/env python3
"""
CLI interface for HT CHPP library.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import click

# Add current directory to path for standalone script execution
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir / "processors"))
sys.path.insert(0, str(current_dir / "utils"))

from client import HTTPClient
from config import Config  
from processors.generic import GenericProcessor


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose: bool):
    """HT CHPP - Clean Hattrick API data processor."""
    setup_logging(verbose)


@cli.command()
@click.argument('endpoint')
@click.option('--league-id', type=int, help='Specific league ID to fetch (worlddetails)')
@click.option('--user-id', type=int, help='Specific user ID to fetch (achievements, managercompendium)')
@click.option('--league-level-unit-id', type=int, help='Specific league level unit ID (leaguedetails)')
@click.option('--output-dir', type=click.Path(), default='./data', help='Output directory for parquet files')
@click.option('--no-timestamp', is_flag=True, help='Disable timestamp suffix on files')
def process(
    endpoint: str,
    league_id: Optional[int],
    user_id: Optional[int],
    league_level_unit_id: Optional[int],
    output_dir: str,
    no_timestamp: bool
):
    """Process any endpoint with output_schema into parquet files."""
    asyncio.run(_process_async(endpoint, league_id, user_id, league_level_unit_id, output_dir, no_timestamp))


async def _process_async(
    endpoint: str,
    league_id: Optional[int],
    user_id: Optional[int],
    league_level_unit_id: Optional[int],
    output_dir: str,
    no_timestamp: bool
):
    """Async implementation of process command."""
    try:
        # Initialize components
        client = HTTPClient.from_env()
        config = Config()
        
        # Validate endpoint
        if endpoint not in config.list_endpoints():
            available = config.list_endpoints()
            click.echo(f"❌ Unknown endpoint '{endpoint}'. Available: {available}", err=True)
            raise click.Abort()
            
        # Check if endpoint has output_schema
        output_schema = config.get_output_schema(endpoint)
        if not output_schema:
            click.echo(f"❌ Endpoint '{endpoint}' doesn't have output_schema configuration", err=True)
            click.echo("   Use 'schema' command to see endpoint structure", err=True)
            raise click.Abort()
        
        # Setup processor
        processor = GenericProcessor(
            endpoint_name=endpoint,
            client=client,
            config=config,
            output_dir=Path(output_dir)
        )
        
        # Prepare parameters based on endpoint
        params = {}
        if league_id is not None:
            params['leagueID'] = league_id
        if user_id is not None:
            if endpoint == 'achievements':
                params['userID'] = user_id
            elif endpoint == 'managercompendium':
                params['userId'] = user_id
        if league_level_unit_id is not None:
            params['leagueLevelUnitID'] = league_level_unit_id
            
        # Process data
        async with client:
            output_files = await processor.process_to_parquet(
                params=params if params else None,
                timestamp_suffix=not no_timestamp
            )
        
        # Report results
        click.echo(f"✅ {endpoint.title()} processing complete!")
        for filepath in output_files:
            click.echo(f"   📄 {filepath}")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()




@cli.command()
def list_endpoints():
    """List available API endpoints."""
    try:
        config = Config()
        endpoints = config.list_endpoints()
        
        click.echo("Available endpoints:")
        for endpoint in endpoints:
            click.echo(f"  • {endpoint}")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('endpoint')
def schema(endpoint: str):
    """Show schema for an endpoint."""
    try:
        config = Config()
        endpoint_config = config.get_endpoint(endpoint)
        latest_version = endpoint_config.get_latest_version()
        schema_data = config.get_endpoint_schema(endpoint, latest_version)
        
        click.echo(f"Schema for {endpoint} (v{latest_version}):")
        click.echo(schema_data)
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()




if __name__ == '__main__':
    cli()