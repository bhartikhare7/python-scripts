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

async def process_stocks(path, exchange):
    try:
        # First, get all existing stocks from Supabase
        response = supabase.table('stocks_search').select('id', 'ticker', 'market_cap').execute()
        existing_stocks = response.data
        #
        # # Create a set of existing stock symbols for faster lookup with market_cap
        existing_symbol_to_id_map = {stock['ticker']: {'id': stock['id'], 'market_cap': stock['market_cap']} for stock in existing_stocks}

        # Read JSON file and collect new stocks
        new_stocks = []
        to_be_updated = []

        with open(path, 'r') as file:
            stocks_data = json.load(file)
            for stock in stocks_data:
                # If stock doesn't exist, add it to new stocks array
                if stock['symbol'] not in existing_symbol_to_id_map.keys():
                    stock_entry = {
                        'ticker': stock['symbol'],
                        'company_name': stock['name'],
                        'sector': stock['sector'],
                        'industry': stock['industry'],
                        'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
                        'exchange': exchange
                    }
                    new_stocks.append(stock_entry)
                else:
                    existing_data = existing_symbol_to_id_map[stock['symbol']]
                    # Only update if market_cap is null in existing data
                    if existing_data['market_cap'] is None:
                        stock_entry = {
                            'id': existing_data['id'],
                            'ticker': stock['symbol'],
                            'company_name': stock['name'],
                            'sector': stock['sector'],
                            'industry': stock['industry'],
                            'market_cap': None if stock['market_cap'] == 'N/A' else stock['market_cap'],
                            'exchange': exchange
                        }
                        to_be_updated.append(stock_entry)

        # Insert new stocks in batches of 1000
        BATCH_SIZE = 1000
        for i in range(0, len(new_stocks), BATCH_SIZE):
            batch = new_stocks[i:i + BATCH_SIZE]
            response = supabase.table('stocks_search').upsert(batch, ignore_duplicates=True).execute()

            if hasattr(response, 'error') and response.error:
                print(f"Error inserting new batch {i//BATCH_SIZE + 1}:", response.error)
            else:
                print(f"Successfully inserted new batch {i//BATCH_SIZE + 1} ({len(batch)} stocks)")

        BATCH_SIZE = 1000
        for i in range(0, len(to_be_updated), BATCH_SIZE):
            batch = to_be_updated[i:i + BATCH_SIZE]
            response = supabase.table('stocks_search').upsert(batch, on_conflict='id').execute()

            if hasattr(response, 'error') and response.error:
                print(f"Error inserting batch {i//BATCH_SIZE + 1}:", response.error)
            else:
                print(f"Successfully inserted batch {i//BATCH_SIZE + 1} ({len(batch)} stocks)")

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
