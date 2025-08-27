from session.cli import handle_session_command
from parser.sql_parser import parse_query
from executor.executor import execute_plan
from semantic.validator import validate_logical_plan
import logging
import os

logging.basicConfig(
    level=logging.CRITICAL,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

if __name__ == "__main__":
    while True:
        # 1. Get SQL statement from client
        query = input("SQL > ")
        if not query.strip():
            continue
        elif query.strip() in ("EXIT", "exit", "QUIT", "quit"):
            exit(0)
        
        # 1a. Handle session level tracing
        if handle_session_command(query):
            continue

        # 2. Parse Query into a logical plan
        try:
            plan = parse_query(query)
        except ValueError as error:
            print("Error:", error)
            exit(1)
        logging.debug(f"{plan}")
        
        # 3. Validate logical plan
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            plan = validate_logical_plan(plan, data_dir)
        except (FileNotFoundError, ValueError) as error:
            print("Error:", error)
            exit(1)
        logging.debug(f"{plan}")

        # 4: Schedule logical plan for execution
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            results = execute_plan(plan, data_dir)
        except (FileNotFoundError, NotImplementedError, ValueError) as error:
            print("Error:", error)
            exit(1)

        # 5. Return result rows to client
        if results.empty:
            print("\nno rows selected.\n")
        else:        
            print("\n", results.to_string(index=False), "\n")