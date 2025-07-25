from parser.sql_parser import parse_query
from executor.executor import execute_plan
from semantic.validator import validate_logical_plan
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

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
    logging.debug(f"{plan}")
    # 3. Validate logical plan
    try:
        plan = validate_logical_plan(plan)
    except (FileNotFoundError, ValueError) as error:
        print("Error:", error)
        exit(1)
    logging.debug(f"{plan}")

    # 4: Schedule logical plan for execution
    results = execute_plan(plan)

    # 5. Return result rows to client
    if not results:
        exit(1)

    for row in results:
        print(row)

    exit(0)