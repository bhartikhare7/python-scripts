import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import json
import asyncio
from merge_stocks_data import process_stocks, fetch_existing_stocks


class TestMergeStocksData(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_json_data = [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 3000000000000
            },
            {
                "symbol": "GOOGL",
                "name": "Alphabet Inc.",
                "sector": "Technology",
                "industry": "Internet Content & Information",
                "market_cap": "N/A"
            },
            {
                "symbol": "TSLA",
                "name": "Tesla Inc.",
                "sector": "Consumer Cyclical",
                "industry": "Auto Manufacturers",
                "market_cap": 800000000000
            }
        ]

        self.existing_stocks_response = {
            "data": [
                {
                    "id": 1,
                    "ticker": "AAPL",
                    "market_cap": 2900000000000
                },
                {
                    "id": 2,
                    "ticker": "MSFT",
                    "market_cap": None
                }
            ]
        }

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_success(self, mock_supabase):
        """Test successful fetching of existing stocks."""
        # Setup mock response
        mock_response = Mock()
        mock_response.data = [
            {"id": 1, "ticker": "AAPL", "market_cap": 3000000000000},
            {"id": 2, "ticker": "GOOGL", "market_cap": None},
            {"id": 3, "ticker": "TSLA", "market_cap": 800000000000}
        ]
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response

        # Execute
        result = fetch_existing_stocks()

        # Verify
        mock_supabase.table.assert_called_once_with('stocks_search')
        mock_supabase.table.return_value.select.assert_called_once_with('id', 'ticker', 'market_cap')
        
        expected_result = {
            'AAPL': {'id': 1, 'market_cap': 3000000000000},
            'GOOGL': {'id': 2, 'market_cap': None},
            'TSLA': {'id': 3, 'market_cap': 800000000000}
        }
        self.assertEqual(result, expected_result)

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_empty_result(self, mock_supabase):
        """Test fetching existing stocks when no stocks exist."""
        # Setup mock response with empty data
        mock_response = Mock()
        mock_response.data = []
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response

        # Execute
        result = fetch_existing_stocks()

        # Verify
        mock_supabase.table.assert_called_once_with('stocks_search')
        self.assertEqual(result, {})

    @patch('merge_stocks_data.supabase')
    def test_fetch_existing_stocks_with_null_values(self, mock_supabase):
        """Test fetching existing stocks with null market cap values."""
        # Setup mock response with null market_cap
        mock_response = Mock()
        mock_response.data = [
            {"id": 1, "ticker": "NVDA", "market_cap": None},
            {"id": 2, "ticker": "AMD", "market_cap": 250000000000}
        ]
        mock_supabase.table.return_value.select.return_value.execute.return_value = mock_response

        # Execute
        result = fetch_existing_stocks()

        # Verify
        expected_result = {
            'NVDA': {'id': 1, 'market_cap': None},
            'AMD': {'id': 2, 'market_cap': 250000000000}
        }
        self.assertEqual(result, expected_result)

    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_new_stocks_only(self, mock_print, mock_file, mock_supabase):
        """Test processing when all stocks are new."""
        # Setup mocks
        mock_file.return_value.read.return_value = json.dumps(self.sample_json_data)
        mock_supabase.table.return_value.select.return_value.execute.return_value = Mock(data=[])
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify
        mock_supabase.table.assert_called_with('stocks_search')
        self.assertEqual(mock_supabase.table.return_value.upsert.call_count, 1)

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_with_existing_stocks(self, mock_print, mock_file, mock_supabase):
        """Test processing with mix of new and existing stocks."""
        # Setup mocks
        mock_file.return_value.read.return_value = json.dumps(self.sample_json_data)
        mock_supabase.table.return_value.select.return_value.execute.return_value = self.existing_stocks_response
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify upsert was called for new stocks
        self.assertEqual(mock_supabase.table.return_value.upsert.call_count, 1)

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_market_cap_na_handling(self, mock_print, mock_file, mock_supabase):
        """Test that 'N/A' market cap values are converted to None."""
        # Setup mocks
        mock_file.return_value.read.return_value = json.dumps([{
            "symbol": "TEST",
            "name": "Test Corp",
            "sector": "Test Sector",
            "industry": "Test Industry",
            "market_cap": "N/A"
        }])
        mock_supabase.table.return_value.select.return_value.execute.return_value = Mock(data=[])
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify upsert was called with None for market_cap
        call_args = mock_supabase.table.return_value.upsert.call_args[0][0]
        self.assertIsNone(call_args[0]['market_cap'])

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_update_null_market_cap(self, mock_print, mock_file, mock_supabase):
        """Test updating existing stocks with null market_cap."""
        # Setup data with stock that exists but has null market_cap
        test_data = [{
            "symbol": "MSFT",
            "name": "Microsoft Corp",
            "sector": "Technology",
            "industry": "Software",
            "market_cap": 2500000000000
        }]
        
        mock_file.return_value.read.return_value = json.dumps(test_data)
        mock_supabase.table.return_value.select.return_value.execute.return_value = self.existing_stocks_response
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify upsert was called twice (once for updates)
        self.assertEqual(mock_supabase.table.return_value.upsert.call_count, 2)

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_batch_processing(self, mock_print, mock_file, mock_supabase):
        """Test that large datasets are processed in batches."""
        # Create large dataset (more than 1000 items)
        large_dataset = []
        for i in range(1500):
            large_dataset.append({
                "symbol": f"STOCK{i}",
                "name": f"Stock {i} Corp",
                "sector": "Technology",
                "industry": "Software",
                "market_cap": 1000000000
            })

        mock_file.return_value.read.return_value = json.dumps(large_dataset)
        mock_supabase.table.return_value.select.return_value.execute.return_value = Mock(data=[])
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify multiple batches were processed (1500 items = 2 batches)
        self.assertEqual(mock_supabase.table.return_value.upsert.call_count, 2)

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_supabase_error(self, mock_print, mock_file, mock_supabase):
        """Test error handling when Supabase operations fail."""
        mock_file.return_value.read.return_value = json.dumps(self.sample_json_data)
        mock_supabase.table.return_value.select.return_value.execute.return_value = Mock(data=[])
        
        # Mock error response
        error_response = Mock()
        error_response.error = "Database connection failed"
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = error_response

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify error was printed
        mock_print.assert_any_call("Error inserting new batch 1:", "Database connection failed")

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('builtins.print')
    async def test_process_stocks_file_not_found(self, mock_print, mock_file, mock_supabase):
        """Test error handling when JSON file is not found."""
        # Execute
        await process_stocks('nonexistent.json', 'nasdaq')

        # Verify error was caught and printed
        mock_print.assert_any_call('Error processing stocks:', 'File not found')

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_invalid_json(self, mock_print, mock_file, mock_supabase):
        """Test error handling when JSON file contains invalid data."""
        # Setup invalid JSON
        mock_file.return_value.read.return_value = "invalid json content"

        # Execute
        await process_stocks('test_path.json', 'nasdaq')

        # Verify error was caught and printed
        self.assertTrue(any('Error processing stocks:' in str(call) for call in mock_print.call_args_list))

    @patch('merge_stocks_data.supabase')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    async def test_process_stocks_exchange_parameter(self, mock_print, mock_file, mock_supabase):
        """Test that exchange parameter is correctly set in stock entries."""
        mock_file.return_value.read.return_value = json.dumps([self.sample_json_data[0]])
        mock_supabase.table.return_value.select.return_value.execute.return_value = Mock(data=[])
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = Mock(error=None)

        # Execute with specific exchange
        await process_stocks('test_path.json', 'nyse')

        # Verify exchange was set correctly
        call_args = mock_supabase.table.return_value.upsert.call_args[0][0]
        self.assertEqual(call_args[0]['exchange'], 'nyse')


if __name__ == '__main__':
    # Run async tests
    def run_async_test(coro):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    # Override test methods to run async
    original_test_methods = []
    for name in dir(TestMergeStocksData):
        if name.startswith('test_') and callable(getattr(TestMergeStocksData, name)):
            original_method = getattr(TestMergeStocksData, name)
            if asyncio.iscoroutinefunction(original_method):
                def make_sync_wrapper(async_method):
                    def sync_wrapper(self):
                        return run_async_test(async_method(self))
                    return sync_wrapper
                setattr(TestMergeStocksData, name, make_sync_wrapper(original_method))

    unittest.main()
