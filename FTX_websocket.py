import websocket
import datetime
import json
import pandas as pd

class FtxStream():
    def __init__(self):
        self.socket = "wss://ftx.com/ws/"
        
        self.minutes_processed_tracker={}
        self.minutes_processed_tracker['BTC/USDT']= {}
        self.minutes_processed_tracker['ETH/USDT']= {}
        self.minutes_processed_tracker['MATIC/USD']= {}
        self.minute_candlesticks_tracker={}
        self.minute_candlesticks_tracker['BTC/USDT']= []
        self.minute_candlesticks_tracker['ETH/USDT']= []
        self.minute_candlesticks_tracker['MATIC/USD']= []
        self.tracker_current={}
        self.tracker_current['BTC/USDT']= {}
        self.tracker_current['ETH/USDT']= {}
        self.tracker_current['MATIC/USD']= {}
        self.tracker_previous={}
        self.tracker_previous['BTC/USDT']= {}
        self.tracker_previous['ETH/USDT']= {}
        self.tracker_previous['MATIC/USD']= {}
        
    def setup_websocket_stream(self):
            websocket.enableTrace(False)
            ws = websocket.WebSocketApp(self.socket,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

            ws.run_forever(ping_interval=10)

    def on_open(self, ws):
        symbols = [ 'ETH/USDT', 'BTC/USDT','MATIC/USD']
        for symbol in symbols:
            data = json.dumps({"op": "subscribe",
                            "channel": "ticker",
                            "market": symbol})
            ws.send(data)

    def on_message(self, ws, message):
        msg_dict=json.loads(message)
        coin_symbol = msg_dict['market']
        self.generate_candlessss(coin_symbol,msg_dict)

    def generate_candlessss(self, symbol, msg_dict):
        self.tracker_previous[symbol] = self.tracker_current[symbol]
        self.tracker_current[symbol] = msg_dict

        date_time = datetime.datetime.fromtimestamp(int(self.tracker_current[symbol]['data']['time']))
        tick_dt = date_time.strftime("%m/%d/%Y %H:%M")

        if not tick_dt in self.minutes_processed_tracker[symbol]:
            self.minutes_processed_tracker[symbol][tick_dt] = True

            if len(self.minute_candlesticks_tracker[symbol]) > 0:
                self.minute_candlesticks_tracker[symbol][-1]['close'] = self.tracker_previous[symbol]['data']['last']

            self.minute_candlesticks_tracker[symbol].append({
                'minute': tick_dt,
                'open': self.tracker_current[symbol]['data']['last'],
                'high': self.tracker_current[symbol]['data']['last'],
                'low': self.tracker_current[symbol]['data']['last']
                })

            df = pd.DataFrame(self.minute_candlesticks_tracker[symbol][:-1])
            if symbol == "ETH/USDT":
                df.to_csv("FTX_ETHUSDT.csv")
            elif symbol == "BTC/USDT":
                df.to_csv("FTX_BTCUSDT.csv")
            elif symbol == "MATIC/USD":
                df.to_csv("FTX_MATICUSDT.csv")
        
        if len(self.minute_candlesticks_tracker[symbol]) > 0:
            current_candlestick = self.minute_candlesticks_tracker[symbol][-1]
            if self.tracker_current[symbol]['data']['last'] > current_candlestick['high']:
                current_candlestick['high'] = self.tracker_current[symbol]['data']['last']
            if self.tracker_current[symbol]['data']['last'] < current_candlestick['low']:
                current_candlestick['low'] = self.tracker_current[symbol]['data']['last']

    def on_error(self, ws, error):
            print(f"Error FTX: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection close : {close_status_code}, {close_msg}")

if __name__ == "__main__":
    ftx_stream = FtxStream()
    ftx_stream.setup_websocket_stream()