Demo of fully async Kafka producer/consumer to store data to SQL database.

## Prepare
Set ENV variables
 - Kafka IP `QUEUE_URLS=0.0.0.0` (or mupliple IPs divied by comma)
 - Postgresql DB `LOGS_DB_URL=postgresql://localhost/postgres`

## Run
 - producer `gunicorn faqstorer.server:app --bind 0.0.0.0:6000 --worker-class aiohttp.worker.GunicornWebWorker`
 - consumer `python -m faqstorer.consumer`
 - send data `curl -X POST -d '{"hello": "world"}' "http://0.0.0.0:6000/store"`