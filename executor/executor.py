import os 
import logging
import pyarrow.parquet as pq
import pandas as pd

def execute_plan(plan, data_dir):
    table_data = {}
    for table in plan.source_tables:
        file_path = os.path.join(data_dir, f"{table}.parquet")
        table_data[table] = pq.read_table(file_path).to_pandas()
    
    # single table query processing
    if len(table_data) == 1:
        table = plan.source_tables[0]
        df = table_data[table]
        proj_cols = []
        for col in plan.col_proj[table]:
            if col in df:
                proj_cols.append(col)
            elif col.lower() in df: 
                proj_cols.append(col.lower())
        df = df[proj_cols]
       
        if plan.filter:
            expressions = plan.filter[table]
            for (col, op, value) in expressions:
                value = value.strip("'\"")

                if value.isdigit():
                    value = int(value)
                
                if op == "=":
                    # For string, need case-insensitive comparison
                    if isinstance(value, str):
                        df = df[df[col].str.lower() == value.lower()]
                    else:
                        df = df[df[col] == value]
                elif op == ">":
                    if isinstance(value, str):
                        raise TypeError(f"Operator {op} not supported with str")
                    df = df[df[col] > value]
                elif op == "<":
                    if isinstance(value, str):
                        raise TypeError(f"Operator {op} not supported with str")
                    df = df[df[col] < value]
                elif op == "<=":
                    if isinstance(value, str):
                        raise TypeError(f"Operator {op} not supported with str")
                    df = df[df[col] >= value]
                elif op == ">=":
                    if isinstance(value, str):
                        raise TypeError(f"Operator {op} not supported with str")
                    df = df[df[col] >= value]
                else:
                    raise NotImplementedError(f"Operator {op} not supported yet.")

                
        if plan.order_by:
            df = df.sort_values(by=plan.order_by, ascending=(plan.order_dir != "DESC"))
        
        return df

