from planner.logical_plan import LogicalPlan

def valid_format(sql_text: str) -> LogicalPlan | None:
    if not sql_text:
        return False
    
    sql_text = sql_text.strip().upper()
    
    if sql_text.endswith(";"):
        sql_text = sql_text[:-1]

    tokens = sql_text.split()

    if not tokens[0] == "SELECT":
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
            order_by_dir = None
    
    col_proj = tokens[sel_idx + 1 : from_idx] # get column projections
    if not col_proj:
        return False
    
    # get source tables
    if where_idx == -1 and oby_idx == -1:
        source_tables = tokens[from_idx + 1 : len(tokens)]
    elif where_idx == -1:
        source_tables = tokens[from_idx + 1 : oby_idx]
    else:
        source_tables = tokens[from_idx + 1 : where_idx]
    
    if not source_tables:
        return False

    # get filters
    filter_clause = None
    if where_idx != -1:
        if oby_idx == -1:
            filter_clause = " ".join(tokens[where_idx + 1: len(tokens)])
        else:
            filter_clause = " ".join(tokens[where_idx + 1: oby_idx])

    # get order by
    order_by = None
    if oby_idx != -1:
        if oby_dir_idx != -1:
            order_by = tokens[oby_idx + 2 : oby_dir_idx]
        else:
            order_by = tokens[oby_idx + 2 : len(tokens)]

    return LogicalPlan(col_proj=col_proj, source_tables=source_tables, 
                        filter=filter_clause, order_by=order_by, order_dir=order_by_dir)


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
    
    print(logical_plan)
    return logical_plan    
