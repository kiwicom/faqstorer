import os

logs_db_url = os.environ.get("LOGS_DB_URL", "postgresql://localhost/postgres")
topic = os.environ.get("TOPIC", 'test')
queues_urls = os.environ.get("QUEUE_URLS", "").split(',')

batch_max_time = float(os.environ.get("BATCH_MAX_TIME", "1"))
batch_max_size = int(os.environ.get("BATCH_MAX_SIZE", "10"))
batch_flush_size = int(os.environ.get("BATCH_FLUSH_SIZE", "5"))

kafka_producer_timeout = int(os.environ.get("KAFKA_PRODUCER_TIMEOUT_MS", "4000"))

batch_sleep = 1
wait_for_databases = 10

# cors settings
allow_headers = ['DNT', 'X-Mx-ReqToken', 'Keep-Alive', 'User-Agent', 'X-Requested-With', 'If-Modified-Since',
                 'Cache-Control', 'Content-Type', 'X-WHOIAM', 'X-WHOIAM-SESSION', 'X-FORTER', 'X-Application']
allow_methods = ['PUT', 'POST', 'GET', 'OPTIONS', 'DELETE']