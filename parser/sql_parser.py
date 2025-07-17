from planner.logical_plan import LogicalPlan

def valid_format(sql_text: str) -> bool:
    
    return True
    pass


def typecheck(sql_text):
    pass

def parse_query(sql_text: str) -> LogicalPlan:

    """Parses a SQL query string into a logical plan.

    Args:
        sql_text (str): query string

    Returns:
        LogicalPlan: root of logical operator tree
    """

    if not valid_format(sql_text):
        raise ValueError("Invalid query format")

    if not typecheck(sql_text):
        raise ValueError("SQL query failed type checks")

    # TODO: Tokenize SQL and construct logical operator tree

