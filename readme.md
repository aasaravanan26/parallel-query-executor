# Mini Parallel Query Engine
A lightweight local parallel query engine that demonstrates distributed-style query execution on local parquet data files.

## Features
- supports queries from .parquet tables from the data/ directory
- basic SQL queries (SELECT... FROM... WHERE... ORDER BY...)
- enables tracing levels (DEBUG, WARNING, ERRORS)
- handles joins between tables
- result caching (Redis)
- fetching results in parallel

## Architecture
- User Query
- Check Redis Cache
  - Cache Hit → Return cached result
  - Cache Miss → Query Planner
- Query Planner
  - Parse Query: convert raw SQL text into tokens, identify clauses (SELECT, FROM, WHERE, ORDER BY)
  - Build Logical Plan: create a LogicalPlan object representing:
    - Column projections
    - Source tables
    - Filter conditions
    - Order by columns and directions
    - Select-all (*) flags
- Semantic Analysis
  - Column validation: check that projected columns exist in the table(s)
  - Table validation: check that referenced tables exist in the database
  - Filter / WHERE clause validation: verify columns in filter exist, operators are valid, data types are compatible
  - ORDER BY validation: check that order-by columns exist and resolve ambiguity across multiple tables
  - Wildcard (*) expansion: expand * into all columns for the table(s)
  - Ambiguous column resolution: map unqualified columns to the correct table when multiple tables are present
  - Data type checks: ensure operators in filter make sense for column types (e.g., don’t compare string with > numeric)
- Query Executor
  - Load tables: read source tables from Parquet files into Pandas DataFrames
  - Apply projections: select only the requested columns
  - Apply filters: filter rows based on WHERE clause conditions
  - Perform joins: merge multiple tables if needed
  - Apply ORDER BY: sort results on specified columns and directions
  - Store results in Redis Cache with expiry
  - return result: final Pandas DataFrame of query results
- Parallel Support
  - Single-table scan & filter runs in parallel using ThreadPoolExecutor
  - Number of workers controlled by SET PARALLEL <N> session command
  - Each worker processes a chunk (partition) of the table’s rows, applies projections and filters, then results are merged back together

## Setup
1. Install dependencies
pip install pandas pyarrow redis

2. Install & run Redis locally
after downloading redis-stable  
make  
src/redis-server

3. Create test data
python setup_test_data.py --n_emp 10000 --n_dept 10000

4. Run the engine
python main.py

## Interactive commands
1. SET TRACE LEVEL [DEBUG | ERROR | CRITICAL | WARNING] (useful for debugging purposes)
2. SET TRACE OFF (to disable tracing)
3. SET CACHE CLEAR (to clear cached query results)
4. SET PARALLEL [NUM_WORKERS]
5. DESC [TABLE_NAME]

## Work In-Progress
1. Parallelism support for multi-table queries (JOINS)
2. Query optimizer - cost based planner based on table's metadata
3. Support for GROUP BY / HAVING / LIMIT / DISTINCT
4. Caching of intermediate results
