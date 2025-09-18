from concurrent.futures import ThreadPoolExecutor
from executor.execute_helper import column_filter
import pandas as pd
import logging
import math
from session import session

def process_chunk(df_chunk, plan):

    """ Process a chunk given to worker.

    Applies WHERE filters and column projections on the chunk of a DataFrame.

    Args:
        df_chunk (pandas.DataFrame): subset of the table.
        plan (LogicalPlan): logical plan containing filters and column projections.

    Returns:
        pandas.DataFrame: filtered and projected chunk of data.
    """

    logging.debug(f"Processing chunk of size {len(df_chunk)}")
    table = plan.source_tables[0]

    # Apply filters
    if plan.single_filters:
        df_chunk = column_filter(plan, df_chunk, table)
    
    # Apply column projection
    proj_cols = [c.lower() for c in plan.col_proj[table] if c.lower() in df_chunk]
    df_chunk = df_chunk[proj_cols]
    
    return df_chunk

# Parallel support for single table scan
def parallel_execute_single_table(plan, df):

    """ Execute a single-table query in parallel.

    Determine chunk size based on number of workers, max chunk size, and number of chunks per worker. 
    Processes each chunk in a ThreadPoolExecutor and concatenate results.
    Apply ORDER BY if specified.

    Args:
        plan (LogicalPlan): logical plan containing filters, projections, and ordering.
        df (pandas.DataFrame): DataFrame representing table.

    Returns:
        pandas.DataFrame: final filtered, projected, and sorted DataFrame.
    """

    # Split data frame into chunks of num_workers size
    logging.debug(f"Initiating parallel scan for dataset of size {len(df)}")
    n = len(df)

    '''
    Derive the number of chunks to split up dataset.
    total number of chunks is num_chunks_per_worker * number of workers.
    chunk size is then size of dataframe / total number of chunks.
    Since size of dataframe >> number of workers in some cases, 
    cap the chunk size to max_chunk_size
    also, ensure that each chunk has atleast one row.
    '''
    num_workers = session.PARALLEL_LEVEL
    max_chunk_size = session.MAX_CHUNK_SIZE
    num_chunks_per_worker = session.NUM_CHUNKS_PER_WORKER
    target_chunks = num_workers * num_chunks_per_worker
    chunk_size = min(max(math.ceil(n / target_chunks), 1), max_chunk_size)
    logging.debug(f"Chunk size per worker: {chunk_size}")

    index_chunks = [df.index[i:i+chunk_size] for i in range(0, n, chunk_size)]
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