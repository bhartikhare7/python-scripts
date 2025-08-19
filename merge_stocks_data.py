import json
from dotenv import load_dotenv
from supabase import create_client, Client

from final_scripts.env import SUPABASE_URL, SUPABASE_KEY

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

def _fetch_existing_stocks():
    """Fetch all existing stocks from Supabase and return as a lookup map."""
    response = supabase.table('stocks_search').select('id', 'ticker', 'market_cap').execute()
    existing_stocks = response.data
    return {stock['ticker']: {'id': stock['id'], 'market_cap': stock['market_cap']} for stock in existing_stocks}

def _create_stock_entry(stock, exchange, stock_id=None):
    """Create a stock entry dictionary from stock data."""
    stock_entry = {
        'ticker': stock['symbol'],
        'company_name': stock['name'],
        'sector': stock['sector'],
        'industry': stock['industry'],
        'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
        'exchange': exchange
    }
    if stock_id is not None:
        stock_entry['id'] = stock_id
    return stock_entry

def _categorize_stocks(stocks_data, existing_symbol_to_id_map, exchange):
    """Categorize stocks into new stocks and stocks to be updated."""
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

def _print_summary(existing_count, new_stocks_count, updated_stocks_count):
    """Print processing summary."""
    print('Summary:')
    print('Existing stocks:', existing_count)
    print('New stocks added:', new_stocks_count)
    print('To be updated:', updated_stocks_count)
    print('Total stocks after operation:', new_stocks_count + updated_stocks_count)

async def process_stocks(path, exchange):
    try:
        # Get existing stocks lookup map
        existing_symbol_to_id_map = _fetch_existing_stocks()

        # Read and categorize stocks from JSON file
        with open(path, 'r') as file:
            stocks_data = json.load(file)
        
        new_stocks, to_be_updated = _categorize_stocks(stocks_data, existing_symbol_to_id_map, exchange)

        # Process new stocks in batches
        _process_batch_upsert(new_stocks, 'stocks_search', 'new')

        # Process updated stocks in batches
        _process_batch_upsert(to_be_updated, 'stocks_search', 'update', 'id')

        # Print summary
        _print_summary(len(existing_symbol_to_id_map), len(new_stocks), len(to_be_updated))

    except Exception as error:
        print('Error processing stocks:', str(error))

if __name__ == "__main__":
    import asyncio
    asyncio.run(process_stocks('../data/stocks/nasdaq_stocks_mc.json', 'nasdaq'))
    #asyncio.run(process_stocks('../data/stocks/nyse_stocks.json', 'nyse'))
