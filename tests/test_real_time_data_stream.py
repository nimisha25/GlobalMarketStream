import pytest
from unittest.mock import patch, MagicMock
from scripts.real_time_data_stream import get_stock_prices, is_market_open
import pytz
import datetime

@patch('scripts.real_time_data_stream.requests')
def test_get_stock_prices_success(mock_requests):
    # Mock the API response
    mock_response = MagicMock()
    mock_response.json.return_value = {'data': 'test'}
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response

    api_key = 'test_api_key'
    index = '^GSPC'

    # Call the function
    data = get_stock_prices(index, api_key)

    # Assertions
    assert data == {'data': 'test'}, "The data returned by get_stock_prices() is incorrect."
    mock_requests.request.assert_called_once()

@patch('scripts.real_time_data_stream.datetime', autospec=True)
def test_is_market_open(mock_datetime):
    market = 'Test Market'
    start_hour = 9
    end_hour = 17
    timezone_str = 'UTC'

    # Mock the current time to be within market hours
    tz = pytz.timezone(timezone_str)
    mock_current_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=tz)

    # Set the mocked datetime.datetime.now() to return a specific datetime object
    mock_datetime.datetime.now.return_value = mock_current_time

    # Call the function
    result = is_market_open(market, start_hour, end_hour, timezone_str)

    # Assertions
    assert result is True, "is_market_open() did not return True when the market should be open."

