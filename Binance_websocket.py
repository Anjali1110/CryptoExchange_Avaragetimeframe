import websocket
import datetime
import json
import pandas as pd

class BinanceStream():
    def __init__(self):
        self.socket = f'wss://stream.binance.com:9443/ws'
        
        self.minutes_processed_tracker={}
        self.minutes_processed_tracker['BTCUSDT']= {}
        self.minutes_processed_tracker['ETHUSDT']= {}
        self.minutes_processed_tracker['MATICUSDT']= {}
        self.minute_candlesticks_tracker={}
        self.minute_candlesticks_tracker['BTCUSDT']= []
        self.minute_candlesticks_tracker['ETHUSDT']= []
        self.minute_candlesticks_tracker['MATICUSDT']= []
        self.tracker_current={}
        self.tracker_current['BTCUSDT']= {}
        self.tracker_current['ETHUSDT']= {}
        self.tracker_current['MATICUSDT']= {}
        self.tracker_previous={}
        self.tracker_previous['BTCUSDT']= {}
        self.tracker_previous['ETHUSDT']= {}
        self.tracker_previous['MATICUSDT']= {}
        
    def setup_websocket_stream(self):
            websocket.enableTrace(False)
            ws = websocket.WebSocketApp(self.socket,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

            ws.run_forever(ping_interval=10)

    def on_open(self, ws):
        data = {
                "method": "SUBSCRIBE",
                "params":
                [
                    "ethusdt@trade",
                    "btcusdt@trade",
                    "maticusdt@trade",
                ],
                "id": 1
        }
        self.send_message(ws, data)
    
    def send_message(self, ws, message_dict):
        data = json.dumps(message_dict).encode()
        ws.send(data)

    def on_message(self, ws, message):
        msg_dict=json.loads(message)
        coin_symbol = msg_dict['s']
        self.generate_candlessss(coin_symbol,msg_dict)

    def generate_candlessss(self, symbol, msg_dict):
        self.tracker_previous[symbol] = self.tracker_current[symbol]
        self.tracker_current[symbol] = msg_dict

        date_time = datetime.datetime.fromtimestamp(int(self.tracker_current[symbol]['T'])/1000)
        tick_dt = date_time.strftime("%m/%d/%Y %H:%M")

        if not tick_dt in self.minutes_processed_tracker[symbol]:
            self.minutes_processed_tracker[symbol][tick_dt] = True

            if len(self.minute_candlesticks_tracker[symbol]) > 0:
                self.minute_candlesticks_tracker[symbol][-1]['close'] = self.tracker_previous[symbol]['p']

            self.minute_candlesticks_tracker[symbol].append({
                'minute': tick_dt,
                'open': self.tracker_current[symbol]['p'],
                'high': self.tracker_current[symbol]['p'],
                'low': self.tracker_current[symbol]['p']
                })

            df = pd.DataFrame(self.minute_candlesticks_tracker[symbol][:-1])
            df.to_csv("Binance_"+symbol+".csv")
        
        if len(self.minute_candlesticks_tracker[symbol]) > 0:
            current_candlestick = self.minute_candlesticks_tracker[symbol][-1]
            if self.tracker_current[symbol]['p'] > current_candlestick['high']:
                current_candlestick['high'] = self.tracker_current[symbol]['p']
            if self.tracker_current[symbol]['p'] < current_candlestick['low']:
                current_candlestick['low'] = self.tracker_current[symbol]['p']

    def on_error(self, ws, error):
            print(f"Error Binance: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection close : {close_status_code}, {close_msg}")


if __name__ == "__main__":
    binance_stream = BinanceStream()
    binance_stream.setup_websocket_stream()