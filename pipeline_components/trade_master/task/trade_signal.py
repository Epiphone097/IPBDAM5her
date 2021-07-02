import talib
import pandas as pd
import utils
import config


class TradeSignal:

    def __init__(self, engine):
        """
        Initializes the detecting of signals
        :param engine: SQLAlchemy Engine
        """
        # Get the patterns to be used in pattern recognition
        self.candle_names = talib.get_function_groups()['Pattern Recognition']
        self.engine = engine
        # Get all the candlesticks
        self.candles = self.get_candles()
        self.add_pattern_recognition()
        # Grab the latest candlestick
        self.current_candle = self.candles.loc[self.candles['date_time'] == self.candles['date_time'].max()]
        self.signal, self.pattern = self.check_signal()

    def get_candles(self):
        """
        Get all the candlesticks from the candlestick table
        :return: Dict:
                    date_time: The Unix timestamp from when the current hour opened (open)
                    open: The opening price of the currency in this hour
                    close: The closing price of the currency in this hour
                    high: The highest price the currency reached during this hour
                    low: The lowest price the currency reached during this hour
        """
        # Get all the candlesticks
        with self.engine.connect() as conn:
            candlestick_result = conn.execute("SELECT * FROM candlestick")
        candles = []
        for row in candlestick_result:
            candles.append({
                'date_time': row.open_timestamp,
                'open': row.open,
                'close': row.close,
                'high': row.high,
                'low': row.low
            })
        # Convert to pandas DataFrame
        candles = pd.DataFrame(candles)
        # Convert Unix timestamp to datetime
        candles['date_time'] = candles['date_time'].astype('float64')
        candles['date_time'] = candles['date_time'].apply(utils.epoch_timestamp_to_date_time)
        return candles

    def add_pattern_recognition(self):
        """
        Start the pattern recognition
        :return: None
        """
        open = self.candles['open']
        high = self.candles['high']
        low = self.candles['low']
        close = self.candles['close']

        # Set for every pattern whether it's -100 (Bearish), 0 (Nothing) or 100 (Bullish)
        for candle in self.candle_names:
            self.candles[candle] = getattr(talib, candle)(open, high, low, close)

    def check_signal(self):
        """
        Check which signal was given
        :return: Integer, String: The given signal (0: nothing, 1: Bullish or 2: Bearish)
        """
        # No pattern: return 0 (do nothing)
        if self.current_candle[self.candle_names].values.sum() == 0:
            return 0, None
        # A single pattern: check what signal and return 1 (bullish) or 2 (bearish)
        elif self.current_candle[self.candle_names].values.sum() == 1:
            # If the value is 100 (Bullish)
            if any(self.current_candle[self.candle_names].values > 0):
                # Get the pattern name and add '_Bull' at the end
                pattern = list((self.current_candle[self.candle_names].keys(), self.current_candle[self.candle_names] != 0))[0] + '_Bull'
                return 1, pattern
            # If the value is -100 (Bearish)
            else:
                # Get the pattern name and add '_Bear' at the end
                pattern = list((self.current_candle[self.candle_names].keys(), self.current_candle[self.candle_names] != 0))[0] + '_Bear'
                return 2, pattern
        # Multiple patterns: get the highest ranking one and check if it's bullish or bearish
        else:
            # Get all the detected patterns
            patterns = list((self.current_candle[self.candle_names].keys(), self.current_candle[self.candle_names] != 0))
            container = []
            # Append the pattern with '_Bull' or '_Bear' to list
            for pattern in patterns:
                if self.current_candle[pattern] > 0:
                    container.append(pattern + '_Bull')
                else:
                    container.append(pattern + '_Bear')
            # Get the detected patterns and their value
            rank_list = [config.PATTERN_RANKING[p] for p in container]
            if len(rank_list) == len(container):
                # Get the highest ranking pattern from the detected ones
                rank_index_best = rank_list.index(min(rank_list))
                pattern = container[rank_index_best]
                # Bad logic, change later; checks if a pattern is Bullish or Bearish
                if any(self.current_candle[pattern] > 0):
                    return 1, pattern
                else:
                    return 2, pattern

    def get_signal(self):
        """
        :return: Integer, String: Detected signal and the detected pattern
        """
        return self.signal, self.pattern