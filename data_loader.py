import pandas as pd
import os

# sample data
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

# save to parquet
base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, "data")
file_path = os.path.join(data_dir, f"dept.parquet")
df.to_parquet(file_path, engine="pyarrow", index=False)