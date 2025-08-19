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

def _fetch_existing_stocks():
    """Fetch existing stocks from Supabase and create lookup map."""
    response = supabase.table('stocks_search').select('id', 'ticker', 'market_cap').execute()
    existing_stocks = response.data
    return {stock['ticker']: {'id': stock['id'], 'market_cap': stock['market_cap']} for stock in existing_stocks}

def _create_stock_entry(stock, exchange, existing_id=None):
    """Create a stock entry dictionary."""
    stock_entry = {
        'ticker': stock['symbol'],
        'company_name': stock['name'],
        'sector': stock['sector'],
        'industry': stock['industry'],
        'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
        'exchange': exchange
    }
    if existing_id:
        stock_entry['id'] = existing_id
    return stock_entry

def _process_stock_data(stocks_data, existing_symbol_to_id_map, exchange):
    """Process stock data and categorize into new stocks and updates."""
    new_stocks = []
    to_be_updated = []
    
    for stock in stocks_data:
        if stock['symbol'] not in existing_symbol_to_id_map:
            stock_entry = _create_stock_entry(stock, exchange)
            new_stocks.append(stock_entry)
        else:
            existing_data = existing_symbol_to_id_map[stock['symbol']]
            if existing_data['market_cap'] is None:
                stock_entry = _create_stock_entry(stock, exchange, existing_data['id'])
                to_be_updated.append(stock_entry)
    
    return new_stocks, to_be_updated

def _insert_stocks_batch(stocks, batch_type="new"):
    """Insert stocks in batches."""
    BATCH_SIZE = 1000
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        if batch_type == "new":
            response = supabase.table('stocks_search').upsert(batch, ignore_duplicates=True).execute()
        else:
            response = supabase.table('stocks_search').upsert(batch, on_conflict='id').execute()
        
        batch_num = i//BATCH_SIZE + 1
        if hasattr(response, 'error') and response.error:
            print(f"Error inserting {batch_type} batch {batch_num}:", response.error)
        else:
            print(f"Successfully inserted {batch_type} batch {batch_num} ({len(batch)} stocks)")

def _update_stocks_batch(stocks):
    """Update existing stocks in batches."""
    _insert_stocks_batch(stocks, "update")

def _print_summary(existing_count, new_stocks_count, updated_stocks_count):
    """Print operation summary."""
    print('Summary:')
    print('Existing stocks:', existing_count)
    print('New stocks added:', new_stocks_count)
    print('To be updated:', updated_stocks_count)
    print('Total stocks after operation:', new_stocks_count + updated_stocks_count)

async def process_stocks(path, exchange):
    try:
        # Fetch existing stocks and create lookup map
        existing_symbol_to_id_map = _fetch_existing_stocks()

        # Read and process JSON file
        with open(path, 'r') as file:
            stocks_data = json.load(file)
            new_stocks, to_be_updated = _process_stock_data(stocks_data, existing_symbol_to_id_map, exchange)

        # Process batches
        _insert_stocks_batch(new_stocks, "new")
        _update_stocks_batch(to_be_updated)
        
        # Print summary
        _print_summary(len(existing_symbol_to_id_map), len(new_stocks), len(to_be_updated))

    except Exception as error:
        print('Error processing stocks:', str(error))

if __name__ == "__main__":
    import asyncio
    asyncio.run(process_stocks('../data/stocks/nasdaq_stocks_mc.json', 'nasdaq'))
    #asyncio.run(process_stocks('../data/stocks/nyse_stocks.json', 'nyse'))
