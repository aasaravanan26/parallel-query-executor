import pandas as pd
import os

# sample data
df_emp = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

df_dept = pd.DataFrame({
    "id": [4, 5, 6],
    "mgr": ["Delta", "Earl", "Fiona"],
    "sal": [100, 200, 300]
})

# save data to parquet
base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, "data")

file_path = os.path.join(data_dir, f"emp.parquet")
df_emp.to_parquet(file_path, engine="pyarrow", index=False)

file_path = os.path.join(data_dir, f"dept.parquet")
df_dept.to_parquet(file_path, engine="pyarrow", index=False)