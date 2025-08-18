from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

from final_scripts.env import SUPABASE_URL, SUPABASE_KEY, ALPHA_VANTAGE_API_KEY
import time
import random
import requests
from typing import Optional, Dict, Any

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_stock_data(symbol: str, session) -> Optional[Dict[str, Any]]:
    """
    Try Yahoo Finance first, then fall back to Alpha Vantage if Yahoo fails
    """
    # First attempt with Yahoo
    yahoo_data = fetch_yahoo_data(symbol, session)
    if yahoo_data:
        return yahoo_data

    print(f"Yahoo Finance failed for {symbol}, trying Alpha Vantage...")
    # If Yahoo fails, try Alpha Vantage
    return fetch_alpha_vantage_data(symbol)


def fetch_alpha_vantage_data(symbol: str, max_retries=1) -> Optional[Dict[str, Any]]:
    """
    Fetch stock data from Alpha Vantage API
    """
    if not ALPHA_VANTAGE_API_KEY:
        print("Alpha Vantage API key not found in environment variables")
        return None

    base_url = "https://www.alphavantage.co/query"

    # We'll need multiple endpoints to get all the data
    endpoints = {
        'overview': {
            'function': 'OVERVIEW',
            'symbol': symbol
        },
        'quote': {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol
        },
        'income': {
            'function': 'INCOME_STATEMENT',
            'symbol': symbol
        },
        'balance': {
            'function': 'BALANCE_SHEET',
            'symbol': symbol
        },
        'cashflow': {
            'function': 'CASH_FLOW',
            'symbol': symbol
        }
    }

    result = {}

    for endpoint_name, params in endpoints.items():
        for attempt in range(max_retries):
            try:
                # Add API key to parameters
                params['apikey'] = ALPHA_VANTAGE_API_KEY

                # Add delay to respect rate limits
                time.sleep(2 + random.uniform(0, 1))

                response = requests.get(base_url, params=params)

                if response.status_code == 429:
                    print(f"Rate limit hit for {endpoint_name}, cooling off...")
                    time.sleep(30 + random.uniform(0, 5))
                    continue

                response.raise_for_status()
                data = response.json()

                # Check for API limit message
                if "Note" in data and "API call frequency" in data["Note"]:
                    print(f"Alpha Vantage API limit reached, cooling off...")
                    time.sleep(30 + random.uniform(0, 5))
                    continue

                result[endpoint_name] = data
                break

            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Error fetching {endpoint_name} data from Alpha Vantage for {symbol}: {e}")
                    return None
                time.sleep(2 ** attempt + random.uniform(0, 1))
                continue

    # Process the collected data into the same format as Yahoo Finance
    return process_alpha_vantage_data(result)


def process_alpha_vantage_data(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process Alpha Vantage data into the same format as Yahoo Finance data
    """
    try:
        if not all(key in data for key in ['overview', 'quote', 'income', 'balance', 'cashflow']):
            return None

        quote_data = data['quote'].get('Global Quote', {})
        overview_data = data['overview']
        income_data = data['income'].get('annualReports', [{}])[0]  # Most recent year
        balance_data = data['balance'].get('annualReports', [{}])[0]
        cashflow_data = data['cashflow'].get('annualReports', [{}])[0]

        return {
            'currentPrice': float(quote_data.get('05. price', 0)),
            'volume': int(quote_data.get('06. volume', 0)),
            'marketCap': float(overview_data.get('MarketCapitalization', 0)),
            'financials': {
                'income': {
                    'totalRevenue': float(income_data.get('totalRevenue', 0)),
                    'netIncome': float(income_data.get('netIncome', 0)),
                    'grossProfit': float(income_data.get('grossProfit', 0)),
                },
                'balance': {
                    'totalAssets': float(balance_data.get('totalAssets', 0)),
                    'totalLiabilities': float(balance_data.get('totalLiabilities', 0)),
                    'totalEquity': float(balance_data.get('totalShareholderEquity', 0)),
                },
                'cashflow': {
                    'operatingCashflow': float(cashflow_data.get('operatingCashflow', 0)),
                    'capitalExpenditures': float(cashflow_data.get('capitalExpenditures', 0)),
                }
            }
        }
    except Exception as e:
        print(f"Error processing Alpha Vantage data: {e}")
        return None


# Modified main processing function
def process_stock(symbol: str, session) -> Optional[Dict[str, Any]]:
    """
    Process a stock using both Yahoo Finance and Alpha Vantage as fallback
    """
    try:
        # Try Yahoo Finance first
        yahoo_data = fetch_yahoo_data(symbol, session)
        if yahoo_data:
            return yahoo_data

        print(f"Yahoo Finance failed for {symbol}, waiting 30 seconds before trying Alpha Vantage...")
        # time.sleep(30)  # Cool off period

        # Try Alpha Vantage as fallback
        alpha_data = fetch_alpha_vantage_data(symbol)
        if alpha_data:
            return alpha_data

        print(f"Both APIs failed for {symbol}")
        return None

    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None


def get_yahoo_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br'
    })

    # Get cookie and crumb
    url = "https://finance.yahoo.com"
    try:
        session.get(url)
        return session
    except Exception as e:
        print(f"Error initializing Yahoo session: {e}")
        return None


def process_financial_value(value: dict) -> float:
    return value.get('raw') if value and 'raw' in value else None


def fetch_yahoo_data(symbol: str, session, max_retries=1) -> dict:
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'

    for attempt in range(max_retries):
        try:
            # Add jitter to avoid synchronized requests
            base_delay = 2 ** attempt
            jitter = random.uniform(0, 0.5)
            delay = base_delay + jitter

            time.sleep(delay)  # Increased delay with exponential backoff

            response = session.get(url)

            if response.status_code == 429:
                # If we hit the rate limit, add a longer cooling off period
                delay = random.uniform(0, 5)
                print(f"we hit the rate limit for YF")
                # time.sleep(30 + delay)
                continue

            response.raise_for_status()
            data = response.json()
            result = data['chart']['result'][0]
            meta = result['meta']
            return {
                'currentPrice': meta['regularMarketPrice'],
                'volume': result['indicators']['quote'][0]['volume'][0]
            }
        except Exception as e:
            if attempt == max_retries - 1:
                print(f'Error fetching basic data for {symbol}: {e}')
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    print(f'Response content: {e.response.text}')
                return None
            time.sleep(base_delay)  # Wait before retrying
    return None


def fetch_detailed_yahoo_data(symbol: str, session, max_retries=5) -> dict:
    url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}'
    params = {
        'modules': 'incomeStatementHistory,balanceSheetHistory,cashflowStatementHistory,defaultKeyStatistics'
    }

    for attempt in range(max_retries):
        try:
            # Add jitter to avoid synchronized requests
            base_delay = 2 ** attempt
            jitter = random.uniform(0, 0.5)
            delay = base_delay + jitter

            time.sleep(delay)

            response = session.get(url, params=params)

            if response.status_code == 429:
                # If we hit the rate limit, add a longer cooling off period
                time.sleep(30 + random.uniform(0, 5))
                continue

            response.raise_for_status()
            data = response.json()
            return data['quoteSummary']['result'][0]
        except Exception as e:
            if attempt == max_retries - 1:
                print(f'Error fetching detailed data for {symbol}: {e}')
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    print(f'Response content: {e.response.text}')
                return None
            continue
    return None


def update_metrics(stock: dict, yahoo_data: dict, detailed_data: dict, data_source: str = 'yahoo'):
    try:
        if data_source == 'yahoo':
            shares_outstanding = process_financial_value(
                detailed_data.get('defaultKeyStatistics', {}).get('sharesOutstanding'))
            pe_ratio = process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('forwardPE'))
            ps_ratio = process_financial_value(
                detailed_data.get('defaultKeyStatistics', {}).get('priceToSalesTrailing12Months'))
            debt_to_equity = process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('debtToEquity'))
            current_price = yahoo_data.get('currentPrice')
            market_cap = current_price * shares_outstanding if current_price and shares_outstanding else None
        else:  # Alpha Vantage
            print(f"AV data for {stock} : {detailed_data}")
            overview_data = detailed_data.get('overview', {})
            current_price = float(detailed_data.get('quote', {}).get('Global Quote', {}).get('05. price', 0))
            market_cap = float(overview_data.get('MarketCapitalization', 0))
            pe_ratio = float(overview_data.get('PERatio', 0)) or None
            ps_ratio = float(overview_data.get('PriceToSalesRatioTTM', 0)) or None
            debt_to_equity = float(overview_data.get('DebtToEquityRatio', 0)) or None

        current_time_utc = datetime.now(timezone.utc)
        metrics_data = {
            'metric_id': f"{stock['ticker']}_{current_time_utc.isoformat()}",
            'stock_id': stock['ticker'],
            'current_price': current_price,
            'market_cap': market_cap,
            'pe_ratio': pe_ratio,
            'ps_ratio': ps_ratio,
            'debt_to_equity_ratio': debt_to_equity,
            'updated_at': current_time_utc.isoformat()
        }

        # Remove None values
        metrics_data = {k: v for k, v in metrics_data.items() if v is not None}

        # Upsert metrics
        supabase.table('stock_metrics').upsert(metrics_data, on_conflict='stock_id').execute()
        print(f"Successfully updated metrics for {stock['ticker']} using {data_source} data")
    except Exception as e:
        print(f'Error updating metrics for {stock["ticker"]}: {e}')
        raise e


def _extract_statements_data(detailed_data: dict, data_source: str):
    """Extract and prepare statements data based on data source"""
    if data_source == 'yahoo':
        income_statements = detailed_data.get('incomeStatementHistory', {}).get('incomeStatementHistory', [])[:4]
        balance_sheets = detailed_data.get('balanceSheetHistory', {}).get('balanceSheetStatements', [])[:4]
        cashflows = detailed_data.get('cashflowStatementHistory', {}).get('cashflowStatements', [])[:4]
    else:  # Alpha Vantage
        income_statements = detailed_data.get('income', {}).get('quarterlyReports', [])[:4]
        balance_sheets = detailed_data.get('balance', {}).get('quarterlyReports', [])[:4]
        cashflows = detailed_data.get('cashflow', {}).get('quarterlyReports', [])[:4]
    
    return zip(income_statements, balance_sheets, cashflows)


def _extract_period_info(income_stmt: dict, data_source: str):
    """Extract period information from income statement"""
    if data_source == 'yahoo':
        if not income_stmt.get('endDate'):
            return None, None, None
        end_date = datetime.fromtimestamp(income_stmt['endDate']['raw'], tz=timezone.utc)
    else:  # Alpha Vantage
        if not income_stmt.get('fiscalDateEnding'):
            return None, None, None
        end_date = datetime.fromisoformat(income_stmt['fiscalDateEnding'])
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    quarter = (end_date.month - 1) // 3 + 1
    year = end_date.year
    return end_date, quarter, year


def _prepare_yahoo_fundamentals(stock: dict, quarter: int, year: int, income_stmt: dict, 
                               balance_sheet: dict, cashflow: dict, detailed_data: dict):
    """Prepare fundamentals data for Yahoo Finance source"""
    return {
        'stock_id': stock['ticker'],
        'period_type': 'QUARTER',
        'quarter': quarter,
        'year': year,
        'revenue': process_financial_value(income_stmt.get('totalRevenue')),
        'expenses': process_financial_value(income_stmt.get('totalOperatingExpenses')),
        'profit': process_financial_value(income_stmt.get('grossProfit')),
        'assets': process_financial_value(balance_sheet.get('totalAssets')),
        'liabilities': process_financial_value(balance_sheet.get('totalLiabilities')),
        'operating_cashflow': process_financial_value(cashflow.get('totalCashFromOperatingActivities')),
        'investing_cashflow': process_financial_value(cashflow.get('totalCashFromInvestingActivities')),
        'financing_cashflow': process_financial_value(cashflow.get('totalCashFromFinancingActivities')),
        'pe_ratio': process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('forwardPE')),
        'ps_ratio': process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('priceToSalesTrailing12Months')),
        'roe': process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('returnOnEquity')),
        'debt_to_equity': process_financial_value(detailed_data.get('defaultKeyStatistics', {}).get('debtToEquity')),
    }


def _prepare_alpha_vantage_fundamentals(stock: dict, quarter: int, year: int, income_stmt: dict, 
                                       balance_sheet: dict, cashflow: dict, detailed_data: dict):
    """Prepare fundamentals data for Alpha Vantage source"""
    print(f"AV fundamental data for {stock} : {detailed_data}")
    return {
        'stock_id': stock['ticker'],
        'period_type': 'QUARTER',
        'quarter': quarter,
        'year': year,
        'revenue': float(income_stmt.get('totalRevenue', 0)) or None,
        'expenses': float(income_stmt.get('totalExpenses', 0)) or None,
        'profit': float(income_stmt.get('grossProfit', 0)) or None,
        'assets': float(balance_sheet.get('totalAssets', 0)) or None,
        'liabilities': float(balance_sheet.get('totalLiabilities', 0)) or None,
        'operating_cashflow': float(cashflow.get('operatingCashflow', 0)) or None,
        'investing_cashflow': float(cashflow.get('cashflowFromInvestment', 0)) or None,
        'financing_cashflow': float(cashflow.get('cashflowFromFinancing', 0)) or None,
        'pe_ratio': float(detailed_data.get('overview', {}).get('PERatio', 0)) or None,
        'ps_ratio': float(detailed_data.get('overview', {}).get('PriceToSalesRatioTTM', 0)) or None,
        'roe': float(detailed_data.get('overview', {}).get('ReturnOnEquityTTM', 0)) or None,
        'debt_to_equity': float(detailed_data.get('overview', {}).get('DebtToEquityRatio', 0)) or None,
    }


def _upsert_fundamentals_record(stock: dict, fundamentals: dict, quarter: int, year: int):
    """Upsert fundamentals record to database"""
    # Check for existing entry
    existing = supabase.table('stock_fundamentals').select('*').eq('stock_id', stock['ticker']) \
        .eq('quarter', quarter).eq('year', year).execute()
    
    fundamentals['created_at'] = datetime.now(timezone.utc).isoformat()
    # Remove None values
    fundamentals = {k: v for k, v in fundamentals.items() if v is not None}
    
    if existing.data:
        # Update existing record
        fundamentals['id'] = existing.data[0]['id']
        supabase.table('stock_fundamentals').update(fundamentals).eq('id', fundamentals['id']).execute()
def _fetch_stocks_from_database():
    """Fetch all stocks from the database"""
    try:
        stocks = supabase.table('stocks').select('*').execute().data
        print(f"all stocks data {stocks}")
        return stocks
    except Exception as e:
        print(f"Error fetching stocks from database: {e}")
        return None


def _check_update_needs(stock: dict):
    """Check if stock needs metrics or fundamentals updates"""
    current_time = datetime.now(timezone.utc)
    
    # Check metrics update need
    metrics = supabase.table('stock_metrics').select('updated_at').eq('stock_id', stock['ticker']).execute()
    if metrics.data:
        last_update = datetime.fromisoformat(metrics.data[0]['updated_at'])
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        needs_metrics = (current_time - last_update) > timedelta(minutes=15)
    else:
        needs_metrics = True

    # Check fundamentals update need
    fundamentals = supabase.table('stock_fundamentals').select('created_at').eq('stock_id', stock['ticker']) \
        .order('created_at', desc=True).limit(1).execute()
    if fundamentals.data:
        last_fundamental = datetime.fromisoformat(fundamentals.data[0]['created_at'])
        if last_fundamental.tzinfo is None:
            last_fundamental = last_fundamental.replace(tzinfo=timezone.utc)
        needs_fundamentals = (current_time - last_fundamental) > timedelta(days=1)
    else:
        needs_fundamentals = True
    
    return needs_metrics, needs_fundamentals


def _fetch_stock_data_with_fallback(stock: dict, session):
    """Fetch stock data using Yahoo Finance with Alpha Vantage fallback"""
    # Try Yahoo Finance first
    yahoo_data = fetch_yahoo_data(stock['ticker'], session)
    if yahoo_data:
        print(f"sleep for 2 sec after YF returns response for {stock}")
        time.sleep(2)
        detailed_data = fetch_detailed_yahoo_data(stock['ticker'], session)
        return yahoo_data, detailed_data, 'yahoo'
    
    # If Yahoo fails, try Alpha Vantage
    print(f"Yahoo Finance failed for {stock['ticker']}, trying Alpha Vantage...")
    alpha_data = fetch_alpha_vantage_data(stock['ticker'])
    if alpha_data:
        print(f"sleep for 15 sec after AV returns response for {stock}")
        time.sleep(15)  # immediately sleep to avoid rate limit
        return alpha_data, alpha_data, 'alpha_vantage'
    
    return None, None, None


def _update_stock_data(stock: dict, yahoo_data: dict, detailed_data: dict, 
                      data_source: str, needs_metrics: bool, needs_fundamentals: bool):
    """Update stock metrics and fundamentals if needed"""
    try:
        if needs_metrics:
            update_metrics(stock, yahoo_data, detailed_data, data_source)
            print(f'Updated metrics for {stock["ticker"]} using {data_source} data')

        if needs_fundamentals:
            update_fundamentals(stock, detailed_data, data_source)
            print(f'Updated fundamentals for {stock["ticker"]} using {data_source} data')

        print(f'Completed processing {stock["ticker"]}')
    except Exception as update_error:
        print(f'Error updating database for {stock["ticker"]}: {update_error}')
        raise update_error


def _process_single_stock(stock: dict, session):
    """Process a single stock for updates"""
    try:
        print(f'Processing {stock["ticker"]}...')
        
        needs_metrics, needs_fundamentals = _check_update_needs(stock)
        
        if needs_metrics or needs_fundamentals:
            yahoo_data, detailed_data, data_source = _fetch_stock_data_with_fallback(stock, session)
            
            if not yahoo_data or not detailed_data:
                print(f'Skipping {stock["ticker"]} - failed to fetch data from both sources')
                return
            
            _update_stock_data(stock, yahoo_data, detailed_data, data_source, needs_metrics, needs_fundamentals)
        else:
            print(f'Skipping {stock["ticker"]} - no update needed')
        
        # Add delay between stocks with some randomization
        time.sleep(3 + random.uniform(0, 2))
        
    except Exception as e:
        print(f'Error processing {stock["ticker"]}: {e}')
        time.sleep(5)


        print(f'Updated fundamentals for {stock["ticker"]} Q{quarter} {year}')
    stocks = _fetch_stocks_from_database()
    if not stocks:

def update_fundamentals(stock: dict, detailed_data: dict, data_source: str = 'yahoo'):
    try:
        statements = _extract_statements_data(detailed_data, data_source)

        for i, (income_stmt, balance_sheet, cashflow) in enumerate(statements):
            end_date, quarter, year = _extract_period_info(income_stmt, data_source)
            if not end_date:
                continue

            # Prepare fundamentals data based on data source
            if data_source == 'yahoo':
                fundamentals = _prepare_yahoo_fundamentals(stock, quarter, year, income_stmt, balance_sheet, cashflow, detailed_data)
            else:
                fundamentals = _prepare_alpha_vantage_fundamentals(stock, quarter, year, income_stmt, balance_sheet, cashflow, detailed_data)

            _upsert_fundamentals_record(stock, fundamentals, quarter, year)

    except Exception as e:
        print(f'Error updating fundamentals for {stock["ticker"]}: {e}')
        raise e


def main():
    try:
        stocks = supabase.table('stocks').select('*').execute().data
        print(f"all stocks data {stocks}")
    except Exception as e:
        print(f"Error fetching stocks from database: {e}")
        return

    # Initialize Yahoo session
    session = get_yahoo_session()
    if not session:
        print("Failed to initialize Yahoo session")
        return

    for stock in stocks:
        try:
            print(f'Processing {stock["ticker"]}...')

            # Check metrics update need
            metrics = supabase.table('stock_metrics').select('updated_at').eq('stock_id', stock['ticker']).execute()

            current_time = datetime.now(timezone.utc)
            if metrics.data:
                last_update = datetime.fromisoformat(metrics.data[0]['updated_at'])
                if last_update.tzinfo is None:
                    last_update = last_update.replace(tzinfo=timezone.utc)
                needs_metrics = (current_time - last_update) > timedelta(minutes=15)
            else:
                needs_metrics = True

            # Check fundamentals update need
            fundamentals = supabase.table('stock_fundamentals').select('created_at').eq('stock_id', stock['ticker']) \
                .order('created_at', desc=True).limit(1).execute()

            if fundamentals.data:
                last_fundamental = datetime.fromisoformat(fundamentals.data[0]['created_at'])
                if last_fundamental.tzinfo is None:
                    last_fundamental = last_fundamental.replace(tzinfo=timezone.utc)
                needs_fundamentals = (current_time - last_fundamental) > timedelta(days=1)
            else:
                needs_fundamentals = True

            if needs_metrics or needs_fundamentals:
                # Try Yahoo Finance first
                yahoo_data = fetch_yahoo_data(stock['ticker'], session)
                if yahoo_data:
                    print(f"sleep for 2 sec after YF returns response for {stock}")
                    time.sleep(2)
                    detailed_data = fetch_detailed_yahoo_data(stock['ticker'], session)
                    data_source = 'yahoo'
                else:
                    # If Yahoo fails, wait and try Alpha Vantage
                    print(f"Yahoo Finance failed for {stock['ticker']}, trying Alpha Vantage...")
                    # time.sleep(30)
                    alpha_data = fetch_alpha_vantage_data(stock['ticker'])
                    if alpha_data:
                        print(f"sleep for 15 sec after AV returns response for {stock}")
                        time.sleep(15)  # immediately sleep to avoid rate limit
                        yahoo_data = alpha_data
                        detailed_data = alpha_data  # Alpha Vantage data is already detailed
                        data_source = 'alpha_vantage'
                    else:
                        detailed_data = None
                        data_source = None

                if not yahoo_data or not detailed_data:
                    print(f'Skipping {stock["ticker"]} - failed to fetch data from both sources')
                    continue

                try:
                    if needs_metrics:
        _process_single_stock(stock, session)


