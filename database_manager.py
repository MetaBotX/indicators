import sqlite3

class DatabaseManager:
    def __init__(self, db_path, indicator_config):
        self.conn = sqlite3.connect(db_path)
        self.indicator_config = indicator_config
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_data (
            timestamp INTEGER PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
        ''')

        # Dynamic creation of indicators table
        indicators_columns = self.get_indicator_columns()
        create_indicators_table_query = f'''
        CREATE TABLE IF NOT EXISTS indicators (
            timestamp INTEGER PRIMARY KEY,
            {', '.join(f'{col_name} REAL' for col_name in indicators_columns)}
        )
        '''
        cursor.execute(create_indicators_table_query)
        self.conn.commit()

    def get_indicator_columns(self):
        columns = []
        if self.indicator_config['rsi']['enabled']:
            columns.append(f"rsi_{self.indicator_config['rsi']['period']}")
        if self.indicator_config['macd']['enabled']:
            columns.extend([
                f"macd_{self.indicator_config['macd']['fast_period']}_{self.indicator_config['macd']['slow_period']}",
                f"macd_signal_{self.indicator_config['macd']['signal_period']}",
                f"macd_histogram_{self.indicator_config['macd']['fast_period']}_{self.indicator_config['macd']['slow_period']}_{self.indicator_config['macd']['signal_period']}"
            ])
        if self.indicator_config['stoch_rsi']['enabled']:
            columns.extend([
                f"stoch_rsi_k_{self.indicator_config['stoch_rsi']['period']}_{self.indicator_config['stoch_rsi']['smooth_k']}",
                f"stoch_rsi_d_{self.indicator_config['stoch_rsi']['period']}_{self.indicator_config['stoch_rsi']['smooth_d']}"
            ])
        if self.indicator_config['ema']['enabled']:
            columns.extend([
                f"ema_{self.indicator_config['ema']['short_period']}",
                f"ema_{self.indicator_config['ema']['long_period']}"
            ])
        columns.extend(["buy_volume", "sell_volume"])
        return columns

    def get_last_timestamp(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT MAX(timestamp) FROM price_data')
        result = cursor.fetchone()[0]
        return result if result else None

    def insert_price_data(self, data):
        cursor = self.conn.cursor()
        cursor.executemany('''
        INSERT OR REPLACE INTO price_data (timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', data)
        self.conn.commit()

    def insert_indicator_data(self, data):
        cursor = self.conn.cursor()
        columns = self.get_indicator_columns()
        placeholders = ', '.join(['?' for _ in columns])
        query = f'''
        INSERT OR REPLACE INTO indicators 
        (timestamp, {', '.join(columns)})
        VALUES (?, {placeholders})
        '''
        cursor.executemany(query, data)
        self.conn.commit()

    def get_price_data(self, limit=None):
        cursor = self.conn.cursor()
        query = 'SELECT * FROM price_data ORDER BY timestamp DESC'
        if limit:
            query += f' LIMIT {limit}'
        cursor.execute(query)
        return cursor.fetchall()

    def __del__(self):
        self.conn.close()