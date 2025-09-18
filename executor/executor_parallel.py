from concurrent.futures import ThreadPoolExecutor
from executor.execute_helper import column_filter
import pandas as pd
import numpy as np 
import logging

def process_chunk(df_chunk, plan):
    logging.debug(f"Processing chunk of size {len(df_chunk)}")
    table = plan.source_tables[0]

    # Apply column projection
    proj_cols = [c.lower() for c in plan.col_proj[table] if c.lower() in df_chunk]
    df_chunk = df_chunk[proj_cols]

    # Apply filters
    if plan.single_filters:
        df_chunk = column_filter(plan, df_chunk, table)
    
    return df_chunk

# Parallel support for single table scan
def parallel_execute_single_table(plan, df, num_workers):
    # Split data frame into chunks of num_workers size
    logging.debug("Initiating parallel scan")
    index_chunks = np.array_split(df.index, num_workers)
    chunks = [df.loc[idx].copy() for idx in index_chunks]

    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # submit the task of each chunk with process_chunk function
        futures = [executor.submit(process_chunk, chunk, plan) for chunk in chunks]
        for future in futures:
            results.append(future.result())

    results = [r for r in results if not r.empty]

    # after filtering, can yield no rows, so return empty df
    if not results:
        return pd.DataFrame(columns=df.columns)
    
    # finally, apply order by if specified
    final_df = pd.concat(results)
    if plan.order_by:
        final_df = final_df.sort_values(by=plan.order_by, ascending=(plan.order_dir != "DESC"))
    return final_df

# Parallel support for multi-table scan
def parallel_execute_multi_table(plan, df, num_workers):
    # TO_DO
    pass