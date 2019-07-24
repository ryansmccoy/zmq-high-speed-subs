import time
from itertools import cycle

import pandas as pd


class FakeFeed:
    def __init__(self, filepath=f'zmq_high_speed/test_data.csv'):
        self.filepath = filepath

    def __iter__(self):
        df = pd.read_csv(self.filepath)
        df.reset_index(drop=True, inplace=True)
        counter = 0
        for idx, num in cycle(df.iterrows()):
            counter += 1
            msg = f"{counter}," + ",".join(list(num.astype(str).to_dict().values()))
            yield msg
            time.sleep(1)
