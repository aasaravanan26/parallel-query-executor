import os 
import logging
import pyarrow.parquet as pq
import pandas as pd

def single_table_execute(plan, table, df):
    df.columns = df.columns.str.lower()

    # only get the column projections
    proj_cols = []
    for col in plan.col_proj[table]:
        if col.lower() in df:
            proj_cols.append(col.lower())
    df = df[proj_cols]

    # WHERE clause, apply filters
    if plan.filter:
        expressions = plan.filter[table]
        for (col, op, value) in expressions:
            value = value.strip("'\"")

            # check whether the value is int/float
            try:
                if "." in value:
                    value = float(value)
                else:
                    value = int(value)
            except:
                pass

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
                df = df[df[col] <= value]
            elif op == ">=":
                if isinstance(value, str):
                    raise TypeError(f"Operator {op} not supported with str")
                df = df[df[col] >= value]
            else:
                raise NotImplementedError(f"Operator {op} not supported yet.")

    # ORDER BY specified, apply it
    if plan.order_by:
        df = df.sort_values(
                    by=[col.lower() for col in plan.order_by],
                    ascending=(plan.order_dir != "DESC")
                )
    
    return df

def execute_plan(plan, data_dir):
    table_data = {}
    for table in plan.source_tables:
        file_path = os.path.join(data_dir, f"{table}.parquet")
        table_data[table] = pq.read_table(file_path).to_pandas()
    
    # single table query processing
    if len(table_data) == 1:
        df = single_table_execute(plan, plan.source_tables[0], table_data[table])
        return df
    