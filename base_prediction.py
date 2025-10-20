import matplotlib.pyplot as plt
from get_df import get_df
import pandas as pd
import numpy as np 

df = get_df('viedenska2014-2020.parquet')

nov_23 = []

for row in df.iloc:
    if '2014-11-23' in str(row['datetime']):
        nov_23.append(row['k_mostu_snp'])
    
    if '2014-11-24' in str(row['datetime']):
        break

plt.plot(range(len(nov_23)), nov_23)
plt.show()