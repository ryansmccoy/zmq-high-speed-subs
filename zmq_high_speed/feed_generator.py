import time
from itertools import cycle
import pandas as pd

class FakeFeed:
    """Generates a Repeating Feed of Data for Benchmarking/Testing"""
    def __init__(self, sleep_time: (int or float) = 1, filepath=f'zmq_high_speed/test_data.csv'):
        self.filepath = filepath
        self.sleep_time = sleep_time

    def __iter__(self):
        df = pd.read_csv(self.filepath)
        df.reset_index(drop=True, inplace=True)
        counter = 0
        for idx, num in cycle(df.iterrows()):
            counter += 1
            msg = f"{counter},    " + ",".join(list(num.astype(str).to_dict().values()))
            yield msg
            time.sleep(self.sleep_time)
