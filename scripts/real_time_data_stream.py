import requests
from kafka import KafkaProducer
import json
import datetime
import time
import pytz
import time
import logging
import os
from dotenv import load_dotenv


def get_stock_prices(index, api_key) :
    url = f"https://yfapi.net/v6/finance/quote"
    querystring = {"symbols" : index}
    
    headers = {'x-api-key' : api_key}
    logging.info("fetching stock data")
    try :
        response = requests.request("GET", url, headers=headers, params=querystring, timeout=10)
        data = response.json()
        logging.info(f"stock data received with response : {response.status_code}")
        # print(data)
        # print(response.text)
        return data
    except requests.exceptions.Timeout:
        logging.ERROR("timeout occurred.")
        raise
    except requests.exceptions.RequestException as e:
        logging.ERROR(f"Request to {url} failed: {e}", exc_info=True)
        raise

def is_market_open(market, start_hour, end_hour, timezone_str):
    tz = pytz.timezone(timezone_str)
    current_time = datetime.datetime.now(tz)
    print(f"current time in {market}: ", current_time)
    market_open_time = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    market_close_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    
    return market_open_time <= current_time <= market_close_time

def kafka_producer(market_info, producer, api_key) :
    market = market_info['name']
    indices = market_info['indices']
    start_hour = market_info['start_hour']
    end_hour = market_info['end_hour']
    timezone_str = market_info['timezone']


    logging.info(f"{market} fucntion called.")
    if not is_market_open(market, start_hour, end_hour, timezone_str) :
        logging.info(f"{market} market is closed.")
        # print(f"{market} is closed.")
        return
    logging.info(f"{market} open")
    # producer = KafkaProducer(
    # bootstrap_servers='localhost:9092',
    # value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
    # )
    for index in indices :
        stock_data = get_stock_prices(index, api_key)
        # transformed_data = transform_data(stock_data)
        # print(transformed_data)
        # print(stock_data)
        logging.info(f"Stock data for {index}: {stock_data}")
        producer.send('stock_data', stock_data)  # Send data to Kafka
        print(f"Sending data for {index}")
        logging.info(f"Sent data for {index}")
            
    producer.flush() #ensures data is sent before sleeping
    logging.info("Producer flush completed.")
    

if __name__ == '__main__' :
    # Configure logging
    logging.basicConfig(
        filename='/Users/nimishamalik/Desktop/Stream-pipeline/logs/stream.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    markets = [
        {'name' : 'USA', 'timezone': 'America/New_York', 'start_hour': 9, 'end_hour': 10, 'indices': ['^GSPC'], 'schedule' : '*/1 * * * *'},
        {'name' : 'India', 'timezone': 'Asia/Kolkata', 'start_hour': 10, 'end_hour': 12, 'indices': ['^BSESN'], 'schedule' : '*/1 * * * *'},
        {'name' : 'Japan','timezone': 'Asia/Tokyo', 'start_hour': 9, 'end_hour': 11, 'indices': ['^N225'], 'schedule' : '*/1 * * * *'},
        {'name' : 'China', 'timezone': 'Asia/Shanghai', 'start_hour': 10, 'end_hour': 12, 'indices': ['^HSI'], 'schedule' : '*/1 * * * *'},
        {'name' : 'Germany', 'timezone': 'Europe/Berlin', 'start_hour': 9, 'end_hour': 18, 'indices': ['^GDAXI'], 'schedule' : '*/1 * * * *'}
    ]
    load_dotenv()
    api_key = os.getenv('api_key')
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
    )
    try : 
        while True :
            for market_info in markets :
                kafka_producer(market_info, producer, api_key)
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        producer.close()
        logging.info("Kafka producer closed.")

    # indices = ['^BSESN']  # S&P 500, Hang Seng, DAX, Nikkei 225, BSE Sensex
    # kafka_producer('India', indices, 1, 23, 'Asia/Kolkata')

