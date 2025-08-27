import os
import logging
import re
from pyarrow import parquet as pq
from collections import defaultdict

def load_table_schema(data_dir, table):
    file_path = os.path.join(data_dir, f"{table}.parquet")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Table {table} not found")
    try:
        schema = pq.read_schema(file_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read schema for table {table}: {str(e)}")
    return set([col.lower() for col in schema.names]), schema

def order_by_validator(plan, data_dir) -> bool:
    # check if columns in order by exists in schema
    order_by_dict = defaultdict(list)
    source = plan.source_tables
    if plan.order_by:
        for col in plan.order_by:
            if '.' in col:
                table_name, col_name = col.split('.', 1)
                if table_name not in source:
                    raise ValueError(f"ORDER BY references unknown table {table_name}")
                file_path = os.path.join(data_dir, f"{table_name}.parquet")
                schema = pq.read_schema(file_path)
                columns = set([c.lower() for c in schema.names])
                if col_name.lower() not in columns:
                    raise ValueError(f"ORDER BY column {col} not found in table {table_name}")
            else:
                for table in source:
                    columns, schema = load_table_schema(data_dir, table)
                    if col.lower() in columns:
                        order_by_dict[col].append(table)
        
        for oby_key, oby_val in order_by_dict.items():
            if len(oby_val) > 1:
                raise ValueError(f"Ambiguous order-by column: {oby_key} found in {oby_val}")
    return True

def where_clause_validator(plan, data_dir) -> bool:
    where_clause = defaultdict(list)
    source = plan.source_tables
    if plan.filter:
        # TO DO: add support for "OR" in chain of where clauses
        clauses = re.split(r"\s+(and|or)\s+", plan.filter, flags=re.IGNORECASE)
        for clause in clauses:
            if 'and' == clause:
                continue
            (col, sign, value) = clause.split(" ")
             # TO DO: prevent case like where name > 5
            if '.' in col:
                table_name, col = col.split('.', 1)
                if table_name not in source:
                    raise ValueError(f"WHERE clause references unknown table {table_name}")
                file_path = os.path.join(data_dir, f"{table_name}.parquet")
                schema = pq.read_schema(file_path)
                columns = set([c.lower() for c in schema.names])
                if col.lower() not in columns:
                    raise ValueError(f"WHERE clause column {col} not found in table {table_name}")
            else:
                for table in source:
                    columns, schema = load_table_schema(data_dir, table)
                    if col.lower() in columns:
                        where_clause[table].append((col, sign, value))
                # TO DO: raise an assert here if a column is not found in any table
        plan.filter = where_clause
    else:
        plan.filter = None
    return True

def validate_logical_plan(plan, data_dir) -> bool:

    """ Validate a logical plan
    
    Description:
        This function verifies whether the following exists:
            1. source tables
            2. columns within source tables
            3. filter columns
            4. order by columns
            5. where predicates

    Args:
        plan (LogicalPlan): logical plan
    Returns:
        LogicalPlan: root of validated logical operator tree
    
    """
    if not plan:
        raise ValueError("No logical plan generated")
    
    source = plan.source_tables
    
    none_cols = None
    if plan.col_proj.get(None) and plan.col_proj[None][0] == '*':
        for table in source:
            if table != None:
                plan.col_proj[table].insert(0, '*')
        if len(plan.col_proj[None]) == 1:
            del plan.col_proj[None]
        else:
            plan.col_proj[None].remove('*')
            none_cols = plan.col_proj[None]
    
    if plan.col_proj.get(None):
        if len(source) == 1:
            plan.col_proj[source[0]] += plan.col_proj[None]
            del plan.col_proj[None]
            none_cols = None
        else:
            none_cols = plan.col_proj[None]
            logging.warning(f"Ambiguous columns {none_cols}, trying to resolve them.")
        
    col_proj = plan.col_proj # defaultdict of column projections of each source
    none_cols_dict = defaultdict(list) # mapping columns: source_tables, to try to resolve ambiguous columns
    if none_cols:
        for col in none_cols:
            _ = none_cols_dict[col]

    # check whether each table/column exists in our database
    for table in source:
        logging.debug(f"Validating table {table} with columns: {col_proj[table]}")

        columns, schema = load_table_schema(data_dir, table)

        # check if columns in column projection exists in schema
        if table in col_proj:
            for col in col_proj[table]:
                if col == '*':
                    plan.col_proj[table] += [col_name.upper() for col_name in schema.names]
                elif col.lower() not in columns:
                    raise ValueError(f"Column {col} not found")
            if '*' in plan.col_proj[table]:
                plan.col_proj[table].remove('*')
        
        # check if any of the columns without source_table can be mapped here
        if none_cols:
            for col in none_cols:
                if col.lower() in columns:
                    none_cols_dict[col].append(table)
    
    # resolve ambiguous columns by appending to column projection of source table
    if none_cols_dict:
        for col, tables in none_cols_dict.items():
            if len(tables) > 1:
                raise ValueError(f"Could not resolve column {col}")
            plan.col_proj[tables[0]].append(col)
                
        if plan.col_proj.get(None):
            del plan.col_proj[None]
        logging.debug("resolved ambiguous columns")

    # check if column projections contain valid source tables and if non_empty 
    source_set = set(source)
    for table in plan.col_proj.keys():
        if table != None and table not in source_set:
            raise ValueError(f"Incorrect table name {table} specified")
    for table in source:
        if len(plan.col_proj[table]) == 0:
            logging.debug(f"Removing table {table} from column projection list and source tables")
            del plan.col_proj[table]
            plan.source_tables.remove(table)
        else:
            # remove duplicate columns
            plan.col_proj[table] = list(set(plan.col_proj[table]))
    
    # validate order by clause
    order_by_validator(plan, data_dir)
    
    # validate where clause
    where_clause_validator(plan, data_dir)

    return plan