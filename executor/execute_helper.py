# This file contains all helper functions needed for EXECUTOR module.

# Helper function to filter a given table based on WHERE predicates
# WHERE predicates are stored in a plan as a dictionary (key = table, value = set of predicates)
def column_filter(plan, df, table):
    expressions = plan.single_filters[table]
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
    return df