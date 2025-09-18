from session.cli import handle_session_command, handle_desc_command
from parser.sql_parser import parse_query
from executor.executor import execute_plan
from semantic.validator import validate_logical_plan
from cache.results_cache import check_results_cache, cache_query
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
        
        # 1b. Handle describe table
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            if handle_desc_command(query, data_dir=data_dir):
                continue
        except (FileNotFoundError, ValueError) as error:
            print(error)
            continue

        # 2a. Check whether SQL text is cached in Redis
        try:
            results = check_results_cache(query)
            if results is not None and not results.empty:
                logging.debug("Fetching results from cache.")
                print("\n", results.to_string(index=False), "\n")
                print(f"\n {len(results)} rows selected.\n")
                continue
        except Exception as e:
            print(f"Redis error: {e}")
            exit(1)

        # 2. Parse Query into a logical plan
        try:
            plan = parse_query(query)
            logging.debug("Parsed query.")
        except ValueError as error:
            print("Error:", error)
            exit(1)
        logging.debug(f"{plan}")
        
        # 3. Validate logical plan
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            plan = validate_logical_plan(plan, data_dir)
            logging.debug("Validated query semantics.")
        except (FileNotFoundError, ValueError) as error:
            print("Error:", error)
            exit(1)
        logging.debug(f"{plan}")

        # 4: Schedule logical plan for execution
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            results = execute_plan(plan, data_dir)
            cache_query(query, results)
        except (FileNotFoundError, NotImplementedError, ValueError, KeyError) as error:
            print("Error:", error)
            exit(1)

        # 5. Return result rows to client
        if results.empty:
            print("\nno rows selected.\n")
        else:        
            print("\n", results.to_string(index=False), "\n")
            print(f"\n {len(results)} rows selected.\n")