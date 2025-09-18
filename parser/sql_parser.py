from planner.logical_plan import LogicalPlan
from collections import defaultdict

def reformat_col_proj(col_proj: str):
    
    """ Parses column projections and maps which table column belongs to (if specified)
    
    Args:
        col_proj (str): column projections
    Returns:
        col_proj_dict (dict): map of source table to column projections
    
    """
    col_proj_dict = defaultdict(list)
    col_proj = col_proj.split(',')
    for col in col_proj:
        if '.' in col: # TABLE_NAME.COLUMN explicitly specified
            table, column = col.split('.', 1)
            col_proj_dict[table.lower()].append(column)
        else: # resolve which column table belongs to in semantic analysis
            col_proj_dict[None].append(col)
    return col_proj_dict

def reformat_source_tables(source_tables: str) -> list:

    """ Parses source tables into list of source tables
    
    Args:
        source_tables (str): source table string
    Returns:
        source_tables (list): list of source tables
    
    """
    source_tables = source_tables.split(',')
    return source_tables

def valid_format(sql_text: str) -> LogicalPlan | None:

    """ Main function to parse a SQL query string into a logical plan.
    
    Args:
        sql_text (str): query string
    Returns:
        LogicalPlan: root of logical operator tree
    
    """

    if not sql_text:
        return False
    
    sql_text = sql_text.strip().upper()
    
    if sql_text.endswith(";"):
        sql_text = sql_text[:-1]

    tokens = sql_text.split()

    if tokens[0] != "SELECT":
        return False
    
    try:
        sel_idx = tokens.index("SELECT") # required
        from_idx = tokens.index("FROM") # required
    except ValueError:
        return None
    
    try:
        where_idx = tokens.index("WHERE") # optional
    except ValueError:
        where_idx = -1

    try:
        oby_idx = tokens.index("ORDER") # optional
        if tokens[oby_idx + 1] != "BY":
            return None
    except (ValueError, IndexError):
        oby_idx = -1

    try:
        oby_dir_idx = tokens.index("ASC") # optional
        order_by_dir = "ASC"
    except ValueError:
        try:
            oby_dir_idx = tokens.index("DESC") # optional
            order_by_dir = "DESC"
        except ValueError:
            oby_dir_idx = -1
            if oby_idx != -1:
                order_by_dir = "ASC"
            else:
                order_by_dir = None

    # get source tables
    if where_idx == -1 and oby_idx == -1:
        source_tables = tokens[from_idx + 1 : len(tokens)]
    elif where_idx == -1:
        source_tables = tokens[from_idx + 1 : oby_idx]
    else:
        source_tables = tokens[from_idx + 1 : where_idx]
    
    if not source_tables:
        return False
    else:
        source_tables = [table.lower() for table in source_tables]
        source_tables = reformat_source_tables("".join(source_tables))

    # get column projection
    col_proj = reformat_col_proj("".join(tokens[sel_idx + 1 : from_idx]))
    if not col_proj:
        return False
    
    # check if select * query
    sel_all = '*' in col_proj[None]
    
    # get filters
    filter_clause = None
    if where_idx != -1:
        if oby_idx == -1:
            filter_clause = " ".join(tokens[where_idx + 1: len(tokens)])
        else:
            filter_clause = " ".join(tokens[where_idx + 1: oby_idx])
        filter_clause = filter_clause.lower()

    # get order by
    order_by = None
    if oby_idx != -1:
        if oby_dir_idx != -1:
            order_by_str = " ".join(tokens[oby_idx + 2 : oby_dir_idx])
        else:
            order_by_str = " ".join(tokens[oby_idx + 2 :])
        
        if order_by_str:
            order_by = [col.strip().lower() for col in order_by_str.split(",") if col.strip()]


    return LogicalPlan(col_proj=col_proj, source_tables=source_tables, 
                        filter=filter_clause, order_by=order_by, order_dir=order_by_dir,
                        sel_all=sel_all)


def parse_query(sql_text: str) -> LogicalPlan:

    """Parses a SQL query string into a logical plan.
    
    Args:
        sql_text (str): query string
    Returns:
        LogicalPlan: root of logical operator tree
    
    """
    logical_plan = valid_format(sql_text)
    if not logical_plan:
        raise ValueError("Invalid query format")
    return logical_plan    
