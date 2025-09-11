import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add the parent directory to the path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from merge_stocks_data import fetch_existing_stocks


class TestFetchExistingStocks(unittest.TestCase):
    """Test cases for fetch_existing_stocks function."""

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_success(self, mock_supabase):
        """Test successful fetching of existing stocks."""
        # Mock response data
        mock_response_data = [
            {'id': 1, 'ticker': 'AAPL', 'market_cap': 2500000000000},
            {'id': 2, 'ticker': 'GOOGL', 'market_cap': 1800000000000},
            {'id': 3, 'ticker': 'MSFT', 'market_cap': 2200000000000}
        ]
        
        # Configure mock
        mock_response = MagicMock()
        mock_response.data = mock_response_data
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response
        
        # Call function
        result = fetch_existing_stocks()
        
        # Verify supabase calls
        mock_supabase.table.assert_called_once_with('stocks_search')
        mock_supabase.table.return_value.select.assert_called_once_with('id', 'ticker', 'market_cap')
        mock_supabase.table.return_value.select.return_value.execute.assert_called_once()
        
        # Verify result structure
        expected_result = {
            'AAPL': {'id': 1, 'market_cap': 2500000000000},
            'GOOGL': {'id': 2, 'market_cap': 1800000000000},
            'MSFT': {'id': 3, 'market_cap': 2200000000000}
        }
        self.assertEqual(result, expected_result)

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_empty_response(self, mock_supabase):
        """Test fetching when no stocks exist."""
        # Mock empty response
        mock_response = MagicMock()
        mock_response.data = []
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response
        
        # Call function
        result = fetch_existing_stocks()
        
        # Verify result is empty dict
        self.assertEqual(result, {})

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_with_null_market_cap(self, mock_supabase):
        """Test fetching stocks with null market cap values."""
        # Mock response with null market cap
        mock_response_data = [
            {'id': 1, 'ticker': 'AAPL', 'market_cap': None},
            {'id': 2, 'ticker': 'GOOGL', 'market_cap': 1800000000000}
        ]
        
        mock_response = MagicMock()
        mock_response.data = mock_response_data
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response
        
        # Call function
        result = fetch_existing_stocks()
        
        # Verify result handles null values correctly
        expected_result = {
            'AAPL': {'id': 1, 'market_cap': None},
            'GOOGL': {'id': 2, 'market_cap': 1800000000000}
        }
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
import json
import os

from supabase import create_client, Client

# Initialize Supabase client
try:
    supabase: Client = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )
except Exception:
    supabase = None

def fetch_existing_stocks():
    """Fetch existing stocks from Supabase and create lookup map."""
    response = supabase.table('stocks_search').select('id', 'ticker', 'market_cap').execute()
    existing_stocks = response.data
    return {stock['ticker']: {'id': stock['id'], 'market_cap': stock['market_cap']} for stock in existing_stocks}

def _process_stock_data(path, exchange, existing_symbol_to_id_map):
    """Process stock data from JSON file and categorize into new and to-be-updated stocks."""
    new_stocks = []
    to_be_updated = []
    
    with open(path, 'r') as file:
        stocks_data = json.load(file)
        for stock in stocks_data:
            stock_entry = {
                'ticker': stock['symbol'],
                'company_name': stock['name'],
                'sector': stock['sector'],
                'industry': stock['industry'],
                'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
                'exchange': exchange
            }
            
            if stock['symbol'] not in existing_symbol_to_id_map.keys():
                new_stocks.append(stock_entry)
            else:
                existing_data = existing_symbol_to_id_map[stock['symbol']]
                # Only update if market_cap is null in existing data
                if existing_data['market_cap'] is None:
                    stock_entry['id'] = existing_data['id']
                    to_be_updated.append(stock_entry)
    
    return new_stocks, to_be_updated

def _insert_stocks_in_batches(stocks, operation_type="new"):
    """Insert or update stocks in batches of 1000."""
    BATCH_SIZE = 1000
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        
        if operation_type == "new":
            response = supabase.table('stocks_search').upsert(batch, ignore_duplicates=True).execute()
        else:
            response = supabase.table('stocks_search').upsert(batch, on_conflict='id').execute()

        if hasattr(response, 'error') and response.error:
            print(f"Error inserting {operation_type} batch {i//BATCH_SIZE + 1}:", response.error)
        else:
            print(f"Successfully inserted {operation_type} batch {i//BATCH_SIZE + 1} ({len(batch)} stocks)")

def _print_summary(existing_count, new_stocks_count, updated_stocks_count):
    """Print summary statistics of the stock processing operation."""
    print('Summary:')
    print('Existing stocks:', existing_count)
    print('New stocks added:', new_stocks_count)
    print('To be updated:', updated_stocks_count)
    print('Total stocks after operation:', new_stocks_count + updated_stocks_count)

async def process_stocks(path, exchange):
    try:
        # Fetch existing stocks from Supabase
        existing_symbol_to_id_map = fetch_existing_stocks()
        
        # Process stock data from file
        new_stocks, to_be_updated = _process_stock_data(path, exchange, existing_symbol_to_id_map)
        
        # Insert new stocks in batches
        _insert_stocks_in_batches(new_stocks, "new")
        
        # Update existing stocks in batches
        _insert_stocks_in_batches(to_be_updated, "update")
        
        # Print summary
        _print_summary(len(existing_symbol_to_id_map), len(new_stocks), len(to_be_updated))

    except Exception as error:
        print('Error processing stocks:', str(error))

if __name__ == "__main__":
    import asyncio
    asyncio.run(process_stocks('../data/stocks/nasdaq_stocks_mc.json', 'nasdaq'))
    #asyncio.run(process_stocks('../data/stocks/nyse_stocks.json', 'nyse'))
