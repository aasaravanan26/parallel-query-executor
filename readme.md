# Mini Parallel Query Engine
A lightweight local parallel query engine that demonstrates distributed-style query execution on local parquet data files.

## Features
- supports queries from .parquet tables from the data/ directory
- basic SQL queries (SELECT... FROM... WHERE... ORDER BY...)
- enables tracing levels (DEBUG, WARNING, ERRORS)
- handles joins between tables
- result caching (Redis)
- fetching results in parallel

## Setup
1. Install dependencies
pip install pandas pyarrow redis

2. Install & run Redis locally
# after downloading redis-stable
make
src/redis-server

3. Create test data
python setup_test_data.py --n_emp 10000 --n_dept 10000

4. Run the engine
python main.py

## Interactive commands
SET TRACE LEVEL [DEBUG | ERROR | CRITICAL | WARNING]
SET TRACE OFF
SET CACHE CLEAR