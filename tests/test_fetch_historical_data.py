import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from scripts.fetch_historical_data import get_data, clean_data, connect_to_sql

@patch('scripts.fetch_historical_data.requests.get')
def test_get_data(mock_get):
    # Mock the response of requests.get
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {}, 
        [
            {"date": "2024", "value": 5000},
            {"date": "2023", "value": 4500}
        ]
    ]
    mock_get.return_value = mock_response

    # Call the function
    indicators = ['FP.CPI.TOTL.ZG']
    countries = ['US']
    df = get_data(indicators, countries)

    # Assertions
    assert isinstance(df, pd.DataFrame), "The returned value is not a DataFrame."
    assert list(df.columns) == ['date', 'value', 'country', 'indicator']
    assert len(df) == 2
    assert df.iloc[0]['country'] == 'US'
    assert df.iloc[0]['indicator'] in ['GDP', 'CPI']  # Corrected to check for either 'GDP' or 'CPI'

def test_clean_data():
    # Create a sample DataFrame without NaN values
    data = {
        'date': ["2024", "2023", "2022"],
        'value': [5000, 4500, 4000],
        'country': ['US', 'CN', 'IN'],
        'indicator': ['CPI', 'GDP', 'CPI']
    }
    df = pd.DataFrame(data)

    # Call the function
    cleaned_df = clean_data(df)

    # Assertions
    assert isinstance(cleaned_df, pd.DataFrame), "The returned value is not a DataFrame."
    assert len(cleaned_df) == 3  # All rows should remain, as there are no NaN values
    assert None not in cleaned_df['value'].values  # Double-check that no None values exist

@patch('scripts.fetch_historical_data.create_engine')
@patch('pandas.DataFrame.to_sql', autospec=True)
def test_connect_to_sql(mock_to_sql, mock_create_engine):
    # Create a sample DataFrame
    data = {
        'date': ["2024", "2023"],
        'value': [5000, 4500],
        'country': ['US', 'CN'],
        'indicator': ['GDP', 'CPI']
    }
    df = pd.DataFrame(data)

    # Call the function
    connect_to_sql(df)

    # Assertions
    mock_create_engine.assert_called_once_with('postgresql+psycopg2://postgres:1234@localhost:5432/mydatabase')
    mock_to_sql.assert_called_once()

