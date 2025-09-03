import numpy as np
import statistics as st

data = [1, 2, 3, 4, 5, 6]
overall_std = np.std(data)
print(f"有偏標準差: {overall_std}")

sample_std = np.std(data, ddof=1)
print(f"無偏標準差: {sample_std}")

std_dev = st.stdev(data)
print(f"樣本標準差(默認為無偏): {std_dev}")
