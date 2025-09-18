# SESSION PARAMETER LIST
PARALLEL_LEVEL: int = 1 # specifies the degree of parallelism for the table scan
MAX_CHUNK_SIZE: int = 50000 # specifies the maximum chunk size given for a worker
NUM_CHUNKS_PER_WORKER: int = 10 # specifies the minimum number of chunks each worker should have
CACHE_EXPIRY_TIME: int = 3600 # 1 hour expiry time in Redis Cache