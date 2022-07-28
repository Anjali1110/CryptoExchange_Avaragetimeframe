import pandas as pd 
import datetime
from Binance_websocket import BinanceStream
from FTX_websocket import FtxStream
from Huobi_websocket import HuobiStreaming
import threading
import time
class concatination():

    def __init__(self):
        self.minutes_processed={}
    
    def getcurrentTimeTracker(self):
        while True:
            tick_dt = datetime.datetime.now().strftime("%m/%d/%Y %H:%M")
            if not tick_dt in self.minutes_processed:
                print("Tick",tick_dt)
                self.minutes_processed[tick_dt] = True
                instruments=['BTCUSDT','ETHUSDT','MATICUSDT']
                
                for instru in instruments:
                    time.sleep(1)
                    self.create_average_1Min_ohlc(instru)
                    if (int(datetime.datetime.now().strftime("%M"))%5==0):
                        self.create_average_5Min_ohlc(instru)  

    def create_average_1Min_ohlc(self,instru):
        try:
            df_binance = pd.read_csv("Binance_{instru}.csv".format(instru=instru), index_col=0)
            df_ftx = pd.read_csv("FTX_{instru}.csv".format(instru=instru), index_col=0)
            df_huobi = pd.read_csv("Huobi_{instru}.csv".format(instru=instru), index_col=0)

            ohlc_averaged_1min_df = pd.DataFrame()
            ohlc_averaged_1min_df.index = df_binance.index
            print("index",df_binance.index)
            ohlc_averaged_1min_df['minute'] = df_binance['minute']
            ohlc_averaged_1min_df['open'] = (df_binance['open']+df_ftx['open']+df_huobi['open'])/3
            ohlc_averaged_1min_df['high'] = (df_binance['high']+df_ftx['high']+df_huobi['high'])/3
            ohlc_averaged_1min_df['low'] = (df_binance['low']+df_ftx['low']+df_huobi['low'])/3
            ohlc_averaged_1min_df['close'] = (df_binance['close']+df_ftx['close']+df_huobi['close'])/3
            ohlc_averaged_1min_df.to_csv(instru+"_1Min_final.csv")
        except:
            print("except")
            time.sleep(60)

    def create_average_5Min_ohlc(self,instru):
        data_5min = pd.read_csv(instru+"_1Min_final.csv", index_col=0, parse_dates=['minute'])
        ohlc_averaged_5min_df = pd.DataFrame()

        data_5min['minute'] = pd.to_datetime(data_5min['minute'])
        data_5min = data_5min.set_index('minute')

        ohlc_averaged_5min_df = data_5min.resample('5T').agg({
            'open':'first',
            'high':'max',
            'low':'min',
            'close':'last'
        })
        ohlc_averaged_5min_df.to_csv(instru+"_5Min_final.csv")


if __name__=="__main__":
    binance_stream = BinanceStream()
    t1 = threading.Thread(target=binance_stream.setup_websocket_stream)
    ftx_stream = FtxStream()
    t2 = threading.Thread(target=ftx_stream.setup_websocket_stream)
    huobi_stream = HuobiStreaming()
    t3 = threading.Thread(target=huobi_stream.setup_websocket_stream)
    concatinations = concatination()
    t4 = threading.Thread(target=concatinations.getcurrentTimeTracker)
    t1.start()
    t2.start()
    t3.start()
    t4.start()