import pandas as pd
import uuid
import utils


class MakeTrade:

    def __init__(self, **kwargs):
        """
        Initialized the trading process
        :param kwargs:
                conn: SQLAlchemy Engine
                signal: Integer (1, 2): The detected signal (1: Bullish, or 2: Bearish) in the latest candlestick
                primary: Integer (0, 1): Whether this is the primary, original trade or not
                original_id: uuid4 or 0: The primary key from the primary, original trade or 0 if it's the primary trade
                doge_amount: Float: The amount of dogecoin which was bought or sold in the primary, original trade
                pattern: String: The detected pattern in the latest candlestick
        """
        self.engine = kwargs.get('conn')
        self.signal = kwargs.get('signal')
        self.primary = kwargs.get('primary')
        self.original_id = kwargs.get('original_id')
        self.doge_amount = kwargs.get('doge_amount')
        self.pattern = kwargs.get('pattern')
        # Create a uuid4 in case this is a primary trade
        self.trade_id = uuid.uuid4()
        self.trade_obj, self.doge_amount = self.set_values()
        self.trade()

    def set_values(self):
        """
        Set the key-values in the trade object (self.trade_ob)
        :return: Dict: trade_obj:
                    trade_id: The surrogate uuid4 key for this trade
                    wallet_id: The surrogate uuid4 key from the used wallet
                    currency_id: The key from the currency which will be traded
                    timestamp: The Unix timestamp in milliseconds when this trade happens
                    eur_value: The value of this trade in Euro's
                    usd_value: The value of this trade in US Dollars
                    doge_value: The value of this trade in Dogecoin
                    wallet_eur_value: The value of the used wallet before/after the trade
                    taker_side: Which side is taken in this trade (0: Seller, 1: Buyer)
                    original: If this is the primary, original trade of the reaction of one
        """
        trade = {'trade_id': self.trade_id}
        # Get the wallet and currency id's
        with self.engine.connect() as conn:
            wallet_result = conn.execute("SELECT wallet_id FROM wallet")
            currency_result = conn.execute("SELECT currency_id FROM currency")
        wallet = []
        for row in wallet_result:
            wallet.append({
                'wallet_id': row.wallet_id
            })
        currency = []
        for row in currency_result:
            currency.append({
                'currency_id': row.currency_id
            })
        trade['wallet_id'] = wallet[0].get('wallet_id')
        trade['currency_id'] = currency[0].get('currency_id')
        # Get the current Unix timestamp in milliseconds
        trade['timestamp'] = utils.get_current_unix_timestamp_ms()
        with self.engine.connect() as conn:
            eur_value = conn.execute("SELECT eur_value FROM wallet")
        wallet = []
        for row in eur_value:
            wallet.append({
                'eur_value': row.eur_value
            })
        # Get the current value of the used wallet BEFORE the trade happens
        trade['wallet_eur_value'] = wallet[0]['eur_value']
        # Set the taker_side, signal, original and the different values based upon whether this is a primary trade or not
        if self.primary:
            # If it's a Bullish signal, become a buyer (ugly, bad logic; change later)
            if self.signal == 1:
                trade['taker_side'] = 1
            # Else become a seller (ugly, bad logic; change later)
            else:
                trade['taker_side'] = 0
            trade['original'] = 1
            trade['eur_value'] = 100.00
            # Convert €100,- to USD and DOGE
            trade['usd_value'] = utils.convert_currency('EUR', 'USD', trade['eur_value'])
            trade['doge_value'] = utils.convert_currency('EUR', 'DOGE', trade['eur_value'])
        else:
            # If it's a Bullish signal, set the signal to Bearish in order to sell (ugly, bad logic; change later)
            if self.signal == 1:
                self.signal = 2
                trade['taker_side'] = 0
            else:
                self.signal = 1
                trade['taker_side'] = 1
            trade['original_id'] = self.original_id
            trade['original'] = 0
            # Convert the given DOGE amount to EUR and USD
            trade['eur_value'] = utils.convert_currency('DOGE', 'EUR', self.doge_amount)
            trade['usd_value'] = utils.convert_currency('DOGE', 'USD', self.doge_amount)
            trade['doge_value'] = self.doge_amount
        return trade, trade['doge_value']

    def get_data(self):
        """
        :return: uuid4: The surrogate key of the this, Float: The amount of DOGE which is traded
        """
        return self.trade_id, self.doge_amount

    def trade(self):
        """
        Execute the trades
        :return: None
        """
        # Round the different value to certain amount of decimals
        self.trade_obj['eur_value'] = round(self.trade_obj['eur_value'], 2)
        self.trade_obj['usd_value'] = round(self.trade_obj['usd_value'], 2)
        self.trade_obj['doge_value'] = round(self.trade_obj['doge_value'], 10)
        with self.engine.connect() as conn:
            # Bullish signal: Remove certain amount from wallet
            if self.signal == 1:
                wallet = conn.execute(
                    f"UPDATE wallet "
                    f"SET eur_value = eur_value - {self.trade_obj['eur_value']}, "
                    f"usd_value = usd_value - {self.trade_obj['usd_value']}, "
                    f"doge_value = doge_value - {self.trade_obj['doge_value']}"
                )
            # Bearish signal: Add certain amount to wallet
            else:
                wallet = conn.execute(
                    f"UPDATE wallet "
                    f"SET eur_value = eur_value + {self.trade_obj['eur_value']}, "
                    f"usd_value = usd_value + {self.trade_obj['usd_value']}, "
                    f"doge_value = doge_value + {self.trade_obj['doge_value']}"
                )
            # Insert a new trade with certain value based upon if this is the primary or secondary trade
            if self.primary:
                trade = conn.execute(
                    f"INSERT INTO trade (trade_id, wallet_id, currency_id, original, pattern, eur_value,"
                    f"usd_value, doge_value, status, success, timestamp, taker_side, wallet_eur_value) "
                    f"VALUES ("
                    f"'{self.trade_id}', "
                    f"'{self.trade_obj['wallet_id']}', "
                    f"'{self.trade_obj['currency_id']}',"
                    f"1, "
                    f"'{self.pattern}', "
                    f"'{self.trade_obj['eur_value']}', "
                    f"'{self.trade_obj['usd_value']}',"
                    f"'{self.trade_obj['doge_value']}', "
                    f"'COMPLETE', "
                    f"1, "
                    f"'{self.trade_obj['timestamp']}', "
                    f"'{self.trade_obj['taker_side']}', "
                    f"'{self.trade_obj['wallet_eur_value']}')"
                    )
            else:
                if self.signal == 1:
                    self.trade_obj['wallet_eur_value'] = float(self.trade_obj['wallet_eur_value']) - self.trade_obj['eur_value']
                else:
                    self.trade_obj['wallet_eur_value'] = float(self.trade_obj['wallet_eur_value']) + self.trade_obj['eur_value']
                trade = conn.execute(
                    f"INSERT INTO trade (trade_id, wallet_id, currency_id, original_id, original, pattern, eur_value,"
                    f"usd_value, doge_value, status, success, timestamp, taker_side, wallet_eur_value) "
                    f"VALUES ("
                    f"'{self.trade_id}', "
                    f"'{self.trade_obj['wallet_id']}', "
                    f"'{self.trade_obj['currency_id']}',"
                    f"'{self.original_id}', "
                    f"0, "
                    f"'{self.pattern}', "
                    f"'{self.trade_obj['eur_value']}', "
                    f"'{self.trade_obj['usd_value']}',"
                    f"'{self.trade_obj['doge_value']}', "
                    f"'COMPLETE', "
                    f"1, "
                    f"'{self.trade_obj['timestamp']}', "
                    f"'{self.trade_obj['taker_side']}', "
                    f"'{self.trade_obj['wallet_eur_value']}')"
                    )