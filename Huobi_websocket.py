import websocket
import datetime
import json
import pandas as pd
import gzip

class HuobiStreaming():
    def __init__(self):
        self.socket = f'wss://api.huobi.pro/ws'
        self.subscribed_pairs = ['btcusdt', 'ethusdt', 'maticusdt']
        
        self.minutes_processed_tracker={}
        self.minutes_processed_tracker['btcusdt']= {}
        self.minutes_processed_tracker['ethusdt']= {}
        self.minutes_processed_tracker['maticusdt']= {}
        self.minute_candlesticks_tracker={}
        self.minute_candlesticks_tracker['btcusdt']= []
        self.minute_candlesticks_tracker['ethusdt']= []
        self.minute_candlesticks_tracker['maticusdt']= []
        self.tracker_current={}
        self.tracker_current['btcusdt']= {}
        self.tracker_current['ethusdt']= {}
        self.tracker_current['maticusdt']= {}
        self.tracker_previous={}
        self.tracker_previous['btcusdt']= {}
        self.tracker_previous['ethusdt']= {}
        self.tracker_previous['maticusdt']= {}
        
    def setup_websocket_stream(self):
            websocket.enableTrace(False)
            ws = websocket.WebSocketApp(self.socket,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

            ws.run_forever()

    def on_open(self, ws):
        i = 0
        for trading_pair in self.subscribed_pairs:
            data = {
                "sub": "market.{}.ticker".format(trading_pair),
                "id": "id"+str(i)
            }
            i += 1
            self.send_message(ws, data)
    
    def send_message(self, ws, message_dict):
        data = json.dumps(message_dict).encode()
        ws.send(data)

    def on_message(self, ws, message):
        unzipped_data = gzip.decompress(message).decode()
        msg_dict = json.loads(unzipped_data)
        if 'ping' in msg_dict:
            data = {
                "pong": msg_dict['ping']
            }
            self.send_message(ws, data)
        elif 'ch' in msg_dict:
            pair_name = msg_dict['ch'].split('.')[1]
            coin_symbol = pair_name
            self.generate_candlessss(coin_symbol,msg_dict)

    def generate_candlessss(self, symbol, msg_dict):
        self.tracker_previous[symbol] = self.tracker_current[symbol]
        self.tracker_current[symbol] = msg_dict

        date_time = datetime.datetime.fromtimestamp(int(self.tracker_current[symbol]['ts'])/1000)
        tick_dt = date_time.strftime("%m/%d/%Y %H:%M")

        if not tick_dt in self.minutes_processed_tracker[symbol]:
            self.minutes_processed_tracker[symbol][tick_dt] = True

            if len(self.minute_candlesticks_tracker[symbol]) > 0:
                self.minute_candlesticks_tracker[symbol][-1]['close'] = self.tracker_previous[symbol]['tick']['lastPrice']

            self.minute_candlesticks_tracker[symbol].append({
                'minute': tick_dt,
                'open': self.tracker_current[symbol]['tick']['lastPrice'],
                'high': self.tracker_current[symbol]['tick']['lastPrice'],
                'low': self.tracker_current[symbol]['tick']['lastPrice']
                })

            df = pd.DataFrame(self.minute_candlesticks_tracker[symbol][:-1])
            if symbol == "ethusdt":
                df.to_csv("Huobi_ETHUSDT.csv")
            elif symbol == "btcusdt":
                df.to_csv("Huobi_BTCUSDT.csv")
            elif symbol == "maticusdt":
                df.to_csv("Huobi_MATICUSDT.csv")
        
        if len(self.minute_candlesticks_tracker[symbol]) > 0:
            current_candlestick = self.minute_candlesticks_tracker[symbol][-1]
            if self.tracker_current[symbol]['tick']['lastPrice'] > current_candlestick['high']:
                current_candlestick['high'] = self.tracker_current[symbol]['tick']['lastPrice']
            if self.tracker_current[symbol]['tick']['lastPrice'] < current_candlestick['low']:
                current_candlestick['low'] = self.tracker_current[symbol]['tick']['lastPrice']

    def on_error(self, ws, error):
            print(f"Error Huobi: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection close : {close_status_code}, {close_msg}")


if __name__ == "__main__":
    huobi_stream = HuobiStreaming()
    huobi_stream.setup_websocket_stream()