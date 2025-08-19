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

def _read_stock_data(path):
    """Read and return stock data from JSON file."""
    with open(path, 'r') as file:
        return json.load(file)

def _create_stock_entry(stock, exchange):
    """Create a stock entry dictionary from stock data."""
    return {
        'ticker': stock['symbol'],
        'company_name': stock['name'],
        'sector': stock['sector'],
        'industry': stock['industry'],
        'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
        'exchange': exchange
    }

def _create_update_entry(stock, existing_data, exchange):
    """Create an update entry dictionary for existing stock."""
    entry = _create_stock_entry(stock, exchange)
    entry['id'] = existing_data['id']
    return entry

def _categorize_stocks(stocks_data, existing_symbol_to_id_map, exchange):
    """Categorize stocks into new stocks and stocks to be updated."""
    new_stocks = []
    to_be_updated = []
    
    for stock in stocks_data:
        if stock['symbol'] not in existing_symbol_to_id_map:
            new_stocks.append(_create_stock_entry(stock, exchange))
        else:
            existing_data = existing_symbol_to_id_map[stock['symbol']]
            if existing_data['market_cap'] is None:
                to_be_updated.append(_create_update_entry(stock, existing_data, exchange))
    
    return new_stocks, to_be_updated

def _process_batch_upsert(stocks, table_name, batch_type, conflict_column=None):
    """Process stocks in batches and upsert to database."""
    BATCH_SIZE = 1000
    
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        
        if conflict_column:
            response = supabase.table(table_name).upsert(batch, on_conflict=conflict_column).execute()
        else:
            response = supabase.table(table_name).upsert(batch, ignore_duplicates=True).execute()
        
        if hasattr(response, 'error') and response.error:
            print(f"Error inserting {batch_type} batch {i//BATCH_SIZE + 1}:", response.error)
        else:
            print(f"Successfully inserted {batch_type} batch {i//BATCH_SIZE + 1} ({len(batch)} stocks)")

async def process_stocks(path, exchange):
    try:
        # First, get all existing stocks from Supabase
        response = supabase.table('stocks_search').select('id', 'ticker', 'market_cap').execute()
        existing_stocks = response.data
        
        # Create a set of existing stock symbols for faster lookup with market_cap
        existing_symbol_to_id_map = {stock['ticker']: {'id': stock['id'], 'market_cap': stock['market_cap']} for stock in existing_stocks}

        # Read and categorize stocks
        stocks_data = _read_stock_data(path)
        new_stocks, to_be_updated = _categorize_stocks(stocks_data, existing_symbol_to_id_map, exchange)

        # Process new stocks
        _process_batch_upsert(new_stocks, 'stocks_search', 'new')
        
        # Process stock updates
        _process_batch_upsert(to_be_updated, 'stocks_search', 'update', 'id')

        # Print summary
        print('Summary:')
        print('Existing stocks:', len(existing_symbol_to_id_map))
        print('New stocks added:', len(new_stocks))
        print('To be updated:', len(to_be_updated))
        print('Total stocks after operation:', len(new_stocks) + len(to_be_updated))

    except Exception as error:
        print('Error processing stocks:', str(error))

if __name__ == "__main__":
    import asyncio
    asyncio.run(process_stocks('../data/stocks/nasdaq_stocks_mc.json', 'nasdaq'))
    #asyncio.run(process_stocks('../data/stocks/nyse_stocks.json', 'nyse'))
