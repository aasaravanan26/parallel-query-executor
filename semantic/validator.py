import os
from pyarrow import parquet as pq

def validate_logical_plan(plan) -> bool:
    if not plan:
        return False
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    
    source = plan.source_tables
    col_proj = plan.col_proj
    order_by_cols = plan.order_by

    # check whether each table/column/order-by exists in our database
    for table in source:
        # define filepath to table
        file_path = os.path.join(data_dir, f"{table}.parquet")
        try:
            schema = pq.read_schema(file_path)
            columns = set([col.lower() for col in schema.names])
        except FileNotFoundError:
            print(f"Table {table} not found")
            return None
        print(schema)
    
    #TODO: both for-loop should be nested in the above for-loop
    for col in col_proj:
        if col != '*' and col.lower() not in columns:
            print(f"Column {col} not found")
            return None

    for col in order_by_cols:
        if col.lower() not in columns:
            print(f"ORDER BY column {col} not found")
            return None

    return True
