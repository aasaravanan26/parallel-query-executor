from parser.sql_parser import parse_query
from executor.executor import execute_plan

if __name__ == "__main__":
    # 1. Get SQL statement from client
    query = None
    while query is None or query.strip() == "":
        query = input("SQL > ")
    
    # 2. Parse Query into a logical plan
    try:
        plan = parse_query(query)
    except ValueError as error:
        print("Error:", error)
        exit(1)
    
    # 3: Schedule logical plan for execution
    results = execute_plan(plan)

    # 4. Return result rows to client
    for row in results:
        print(row)

    exit(0)