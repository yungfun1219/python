import Numpy as np
import pandas as pd
import matplotlib.pyplot as plt
scores = {'Math':[90,50,70,80],
          'English':[60,70,90,50],
          'History':[33,75,88,60],
          'Chinese':[22,58,66,37]}
df = pd.DataFrame(scores, index = ['Simon','Allen','Jimmy','Peter'])
df.plot()