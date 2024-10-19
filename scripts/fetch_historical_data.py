import pandas as pd
import requests
from sqlalchemy import create_engine 

def get_data(indicators, countries) :
    all_data = []

    for country in countries :
        for indicator in indicators :
            url =f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&date=2004:2024"
            response = requests.get(url)
            json_r = response.json()
    
            if response.status_code == 200 and len(json_r) > 1:
                data = json_r[1]
                df = pd.DataFrame(data)
        
                df['country'] = country
                parts = indicator.split(".")
                df['indicator'] = parts[1]
                
                all_data.append(df[['date', 'value', 'country', 'indicator']])
            else :
                print(f"Failed to fetch new data for {country}, {indicator}")
                continue
    final_df = pd.concat(all_data, ignore_index=True)
    return final_df

def clean_data(df) :
    df = df[['date', 'value', 'country', 'indicator']].dropna(subset=['value'])
    return df

def connect_to_sql(df) :  
    try :
        engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/mydatabase')
        df.to_sql('historical_gdp_cpi_data', engine, index = False, if_exists='append')
    except Exception as e :
        print(f'error ecoored{e}')

if __name__ == "__main__" :
    df = get_data(['FP.CPI.TOTL.ZG', 'NY.GDP.MKTP.CD'], ['US', 'CN', 'DE', 'JP', 'IN'])
    df = clean_data(df)
    connect_to_sql(df)