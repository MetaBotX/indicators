import time
from config_loader import load_config
from database_manager import DatabaseManager
from binance_fetcher import BinanceFetcher
from indicator_calculator import IndicatorCalculator

def get_db_name(config):
    symbol = config['symbol'].replace('/', '-').lower()
    return config['database_name_format'].format(timeframe=config['timeframe'], symbol=symbol)

def calculate_and_store_indicators(db_manager, config):
    price_data = db_manager.get_price_data()
    if not price_data:
        print("No price data available to calculate indicators.")
        return

    calculator = IndicatorCalculator(price_data, config)
    indicator_data = calculator.calculate_indicators()
    db_manager.insert_indicator_data(indicator_data)
    print(f"Calculated and stored indicators for {len(indicator_data)} data points.")

def main():
    config = load_config()
    db_name = get_db_name(config)
    db_manager = DatabaseManager(db_name, config['indicators'])
    binance_fetcher = BinanceFetcher(config['binance_api_key'], config['binance_secret'])

    symbol = config['symbol']
    timeframe = config['timeframe']

    # Initial data fetch
    last_timestamp = db_manager.get_last_timestamp()
    if last_timestamp is None:
        since = binance_fetcher.get_timestamp_days_ago(config['initial_fetch_days'])
    else:
        since = (last_timestamp + 1) * 1000  # Convert to milliseconds

    initial_data = binance_fetcher.fetch_data(symbol, timeframe, since)
    db_manager.insert_price_data(initial_data)
    print(f"Initial data fetch complete. {len(initial_data)} data points inserted.")

    # Calculate and store indicators for initial data
    calculate_and_store_indicators(db_manager, config)

    # Continuous update
    while True:
        try:
            last_timestamp = db_manager.get_last_timestamp()
            since = (last_timestamp + 1) * 1000  # Convert to milliseconds

            new_data = binance_fetcher.fetch_data(symbol, timeframe, since)
            if new_data:
                db_manager.insert_price_data(new_data)
                print(f"Updated with {len(new_data)} new data points.")
                calculate_and_store_indicators(db_manager, config)
            else:
                print("No new data available.")

            time.sleep(config['update_interval'])
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(config['update_interval'])

if __name__ == "__main__":
    main()