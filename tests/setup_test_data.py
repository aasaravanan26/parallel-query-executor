import pandas as pd
import os
import random
import string
import argparse

def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length)).capitalize()

def setup_test_data(n_emp=10000, n_dept=10000):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Generate employees
    emp_data = {
        "id": list(range(1, n_emp + 1)),
        "name": [random_name() for _ in range(n_emp)],
        "age": [random.randint(20, 60) for _ in range(n_emp)]
    }
    df_emp = pd.DataFrame(emp_data)

    # Generate departments
    dept_data = {
        "id": list(range(1, n_dept + 1)),
        "mgr": [random_name() for _ in range(n_dept)],
        "sal": [random.randint(50_000, 200_000) for _ in range(n_dept)]
    }
    df_dept = pd.DataFrame(dept_data)

    # Normalize column names to lowercase
    df_emp.columns = df_emp.columns.str.lower()
    df_dept.columns = df_dept.columns.str.lower()

    # Write to Parquet
    def write_parquet(df, name):
        file_path = os.path.join(data_dir, f"{name}.parquet")
        df.to_parquet(file_path, engine="pyarrow", index=False)
        print(f"✅ Wrote {file_path} ({len(df)} rows)")

    write_parquet(df_emp, "emp")
    write_parquet(df_dept, "dept")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test Parquet data")
    parser.add_argument("--n_emp", type=int, default=10000, help="Number of employee rows")
    parser.add_argument("--n_dept", type=int, default=10000, help="Number of department rows")
    args = parser.parse_args()

    setup_test_data(args.n_emp, args.n_dept)
