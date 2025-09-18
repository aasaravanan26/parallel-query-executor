import os 
import logging
import pyarrow.parquet as pq
import pandas as pd
from executor.executor_parallel import parallel_execute_single_table
from executor.execute_helper import column_filter
from session import session

def single_table_execute(plan, table, df):

    """ Execute plan on a single table.

    Applies column projections, WHERE clause filters, and ORDER BY to table specified.

    Args:
        plan (LogicalPlan): logical plan containing filters, projections, and ordering.
        table (str): source table name
        df (pandas.DataFrame): DataFrame representing the table data.

    Returns:
        pandas.DataFrame: Filtered, projected, and sorted DataFrame for the table.
    """

    df.columns = df.columns.str.lower()

    # WHERE clause, apply single filters
    if plan.single_filters:
        df = column_filter(plan, df, table)

    # only get the column projections
    proj_cols = []
    for col in plan.col_proj[table]:
        if col.lower() in df:
            proj_cols.append(col.lower())
    df = df[proj_cols]

    # ORDER BY specified, apply it
    if plan.order_by:
        df = df.sort_values(
                    by=[col.lower() for col in plan.order_by],
                    ascending=(plan.order_dir != "DESC")
                )
    
    return df

def multi_table_execute(plan, tables, df_arr):

    """Execute plan across multiple tables.

    Handles single-table filters, column projections, join filters, and cross joins.
    Performs inner joins for equi-joins or cross joins if no join conditions are present.
    Applies ORDER BY sorting after joining tables together.

    Args:
        plan (LogicalPlan): logical plan containing filters, projections, join info, and ordering.
        tables (list[str]): list of source tables
        df_arr (dict[str, pandas.DataFrame]): map of source tables to its DataFrames.

    Returns:
        pandas.DataFrame: resulting DataFrame after joins, filters, projections, and sorting.

    Raises:
        NotImplementedError: if a non-equi join is requested.
    """

    for table in df_arr.keys():
        df_arr[table].columns = df_arr[table].columns.str.lower()

    # WHERE clause, apply single filters
    if plan.single_filters:
        for table, expressions in plan.single_filters.items():
            df_arr[table] = column_filter(plan, df_arr[table], table)
    
    # only get the column projections
    for table in tables:
        df = df_arr[table]
        proj_cols = [c.lower() for c in plan.col_proj[table] if c.lower() in df.columns]
        if proj_cols:
            df_arr[table] = df[proj_cols]
    
    joined_df = None
    if plan.join_filters:
        joined_df = df_arr[tables[0]]
        for (t1, c1, op, t2, c2) in plan.join_filters:
            if op != '=':
                raise NotImplementedError(f"Only equi-joins supported now")
            
            # perform inner-join between tables
            joined_df = pd.merge(joined_df,
                                df_arr[t2],
                                left_on=c1.lower(),
                                right_on=c2.lower(),
                                suffixes=(f"_{t1}", f"_{t2}"))
    else:
        # do a cross join on all tables
        for table in tables:
            df_arr[table].columns = [f"{table}.{col}" for col in df_arr[table].columns]
        dfs = [df_arr[t] for t in tables]
        joined_df = dfs[0]
        for next_df in dfs[1:]:
            joined_df = joined_df.merge(next_df, how='cross')
    
    if plan.order_by:
        joined_df = joined_df.sort_values(
            by=[c.lower() for c in plan.order_by],
            ascending=(plan.order_dir != "DESC")
        )
    return joined_df

def execute_plan(plan, data_dir):

    """Execute a logical plan 
    
    Load table data and use either single or multi-table execution based on session's PARALLEL_LEVEL
    Currently, parallel execution for multiple tables is not supported.

    Args:
        plan (LogicalPlan): logical plan of the query
        data_dir (str): directory containing parquet table files.

    Returns:
        pandas.DataFrame: final query result as a DataFrame.

    Raises:
        NotImplementedError: if parallel execution is used for multiple tables.
    """

    table_data = {}
    for table in plan.source_tables:
        file_path = os.path.join(data_dir, f"{table}.parquet")
        table_data[table] = pq.read_table(file_path).to_pandas()
    
    logging.debug(f"Executing with parallelism {session.PARALLEL_LEVEL}")
    parallel = session.PARALLEL_LEVEL
    
    # single table query processing
    if len(table_data) == 1 and parallel == 1:
        df = single_table_execute(plan, plan.source_tables[0], table_data[table])
    elif len(table_data) == 1:
        df = parallel_execute_single_table(plan, table_data[table])
    elif parallel == 1:
        df = multi_table_execute(plan, plan.source_tables, table_data)
    else:
        raise NotImplementedError("Parallel support for multiple tables not implemented.")
    return df
